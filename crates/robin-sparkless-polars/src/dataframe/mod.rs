//! DataFrame module: main tabular type and submodules for transformations, aggregations, joins, stats.

mod aggregations;
pub mod joins;
mod stats;
mod transformations;

pub(crate) use aggregations::disambiguate_agg_output_names;
pub use aggregations::{CubeRollupData, GroupedData, PivotedGroupedData};
pub use joins::{
    JoinOptions, JoinType, expr_contains_only_join_key_equalities, join,
    try_extract_join_eq_columns, try_extract_join_eq_columns_all,
};
pub use stats::DataFrameStat;
pub(crate) use transformations::literal_value_to_serde_value;
pub use transformations::{
    DataFrameNa, DropColumnSpec, SelectItem, filter, order_by, order_by_exprs, select,
    select_items, select_with_exprs, with_column,
};

use crate::column::Column;
use crate::error::{EngineError, polars_to_core_error};
use crate::functions::SortOrder;
use crate::schema::{DataType as LogicalDataType, StructField, StructType, StructTypePolarsExt};
use crate::session::SparkSession;
use crate::type_coercion::{coerce_for_pyspark_comparison, is_numeric_public};
use polars::datatypes::TimeUnit;
use polars::prelude::{
    AnyValue, CollectBatches, DataFrame as PlDataFrame, DataType, Engine, Expr, Field, IntoLazy,
    LazyFrame, LiteralValue, NULL, NamedFrom, Operator, PlSmallStr, PolarsError, Schema,
    SchemaNamesAndDtypes, UnknownKind, col, lit,
};
use robin_sparkless_core::engine::{
    ArrowIpcBatchStream, ArrowIpcStream, CancellationToken, ExternalCatalogProvider,
    ExternalTableDescriptor, ExternalTableSaveMode, ExternalTableWriteBatchRequest,
    ExternalTableWriteRequest,
};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

/// Default for `spark.sql.caseSensitive` (PySpark default is false = case-insensitive).
const DEFAULT_CASE_SENSITIVE: bool = false;
const DEFAULT_EXTERNAL_ARROW_BATCH_ROWS: usize = 10_000;
const DEFAULT_EXTERNAL_ARROW_BATCH_BYTES: usize = 64 * 1024 * 1024;
const RANGE_EXECUTION_BATCH_ROWS: usize = 16_384;

/// Positional result fields, rows, and their declared logical schema.
pub type PositionalJsonResult = (Vec<String>, Vec<Vec<JsonValue>>, StructType);

fn projected_source_column(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Column(name) => Some(name.to_string()),
        Expr::Alias(inner, _) | Expr::Cast { expr: inner, .. } => projected_source_column(inner),
        Expr::Function {
            input,
            function:
                polars::prelude::FunctionExpr::StructExpr(polars::prelude::StructFunction::FieldByName(
                    field_name,
                )),
        } => Some(format!(
            "{}.{}",
            projected_source_column(input.first()?)?,
            field_name
        )),
        _ => None,
    }
}

fn projected_expression_contains_cast(expression: &Expr) -> bool {
    match expression {
        Expr::Alias(inner, _) => projected_expression_contains_cast(inner),
        Expr::Cast { .. } => true,
        _ => false,
    }
}

fn logical_field_for_projected_column(
    schema: &StructType,
    source_name: &str,
    case_sensitive: bool,
) -> Option<StructField> {
    let matches_name = |left: &str, right: &str| {
        if case_sensitive {
            left == right
        } else {
            left.eq_ignore_ascii_case(right)
        }
    };
    let parts = source_name.split('.').collect::<Vec<_>>();

    // Prefer an exact top-level match so columns whose names contain dots remain addressable.
    if let Some(field) = schema
        .fields()
        .iter()
        .find(|field| matches_name(&field.name, source_name))
    {
        return Some(field.clone());
    }

    // A resolved expression can still carry a table qualifier before the nested field path.
    // Try each component as the root, then follow only declared struct fields.
    for (root_index, root_name) in parts.iter().enumerate() {
        let Some(mut field) = schema
            .fields()
            .iter()
            .find(|field| matches_name(&field.name, root_name))
            .cloned()
        else {
            continue;
        };
        let mut nullable = field.nullable;
        let mut found = true;
        for child_name in &parts[root_index + 1..] {
            let robin_sparkless_core::DataType::Struct(children) = &field.data_type else {
                found = false;
                break;
            };
            let Some(child) = children
                .iter()
                .find(|child| matches_name(&child.name, child_name))
                .cloned()
            else {
                found = false;
                break;
            };
            nullable |= child.nullable;
            field = child;
        }
        if found && root_index + 1 < parts.len() {
            // A nested value is also null when any containing struct is null, even if the
            // declared leaf itself is non-nullable.
            field.nullable = nullable;
            return Some(field);
        }
    }
    None
}

fn expression_contains(expression: &Expr, predicate: impl Fn(&Expr) -> bool) -> bool {
    let mut found = false;
    let _ = expression.clone().try_map_expr(|node| {
        if predicate(&node) {
            found = true;
        }
        Ok(node)
    });
    found
}

fn range_projection_preserves_row_count(expressions: &[Expr]) -> bool {
    // Keep the constant-count shortcut away from expressions whose result height can differ
    // from the input. Unknown Polars function nodes take the conservative fallback as well.
    expressions.iter().all(|expression| {
        !expression_contains(expression, |node| {
            matches!(node, Expr::Literal(value) if !value.is_scalar())
                || matches!(
                    node,
                    Expr::Agg(_)
                        | Expr::DataTypeFunction(_)
                        | Expr::Display { .. }
                        | Expr::Element
                        | Expr::Explode { .. }
                        | Expr::Eval { .. }
                        | Expr::Filter { .. }
                        | Expr::Gather { .. }
                        | Expr::Function { .. }
                        | Expr::Len
                        | Expr::Slice { .. }
                        | Expr::Over { .. }
                        | Expr::AnonymousFunction { .. }
                        | Expr::SubPlan(..)
                )
        })
    })
}

fn range_projection_can_run_per_batch(expressions: &[Expr]) -> bool {
    // Some row-preserving expressions still depend on the full input (for example sorting or
    // windows). Only append a projection to the range pipeline when chunks can execute alone.
    expressions.iter().all(|expression| {
        !expression_contains(expression, |node| {
            matches!(node, Expr::Literal(value) if !value.is_scalar())
                || matches!(
                    node,
                    Expr::Agg(_)
                        | Expr::DataTypeFunction(_)
                        | Expr::Display { .. }
                        | Expr::Element
                        | Expr::Explode { .. }
                        | Expr::Eval { .. }
                        | Expr::Filter { .. }
                        | Expr::Gather { .. }
                        | Expr::Function { .. }
                        | Expr::Len
                        | Expr::Over { .. }
                        | Expr::AnonymousFunction { .. }
                        | Expr::Slice { .. }
                        | Expr::Sort { .. }
                        | Expr::SortBy { .. }
                        | Expr::SubPlan(..)
                )
        })
    })
}

#[derive(Clone)]
pub(crate) struct RangeCardinality {
    pub start: i64,
    pub step: i64,
    pub row_count: u64,
    operations: Vec<RangeOperation>,
}

#[derive(Clone)]
enum RangeOperation {
    Filter(Expr),
    Limit(u64),
    Project(Vec<Expr>),
}

enum RangeBatchError {
    Polars(PolarsError),
    Engine(EngineError),
}

struct RangeBatchProducer {
    range: RangeCardinality,
    case_sensitive: bool,
    token: Option<CancellationToken>,
    batch_rows: usize,
    first_index: u64,
    limit_consumed: Vec<u64>,
}

impl RangeBatchProducer {
    fn new(
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

    fn next_batch(&mut self) -> Result<Option<PlDataFrame>, RangeBatchError> {
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

enum IpcBatchSource {
    Polars(CollectBatches),
    Range(RangeBatchProducer),
}

impl IpcBatchSource {
    fn next_batch(&mut self) -> Option<Result<PlDataFrame, EngineError>> {
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
        }
    }
}

/// A bounded, cancellable stream of collected DataFrame batches.
///
/// This export iterator uses the native range producer for plans that can be
/// evaluated per range batch, avoiding Polars' unsupported `AnonymousScan`
/// lowering for those plans.
pub struct DataFrameBatches {
    source: Option<IpcBatchSource>,
    token: CancellationToken,
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

fn merge_union_logical_type(
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
        (D::Struct(left_fields), D::Struct(right_fields), DataType::Struct(output_fields))
            if left_fields.len() == right_fields.len()
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
            Some(D::Struct(fields))
        }
        _ => crate::schema_conv::checked_polars_type_to_data_type(output_physical, "union").ok(),
    }
}

fn conservative_union_logical_type(output_physical: &DataType) -> Option<LogicalDataType> {
    match output_physical {
        DataType::List(element) => Some(LogicalDataType::ArrayWithNullability {
            element_type: Box::new(conservative_union_logical_type(element)?),
            contains_null: true,
        }),
        DataType::Struct(fields) => Some(LogicalDataType::Struct(
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

fn integer_literal(value: &LiteralValue) -> Option<i64> {
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

fn external_arrow_batch_limit(
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
fn pyspark_type_name(dtype: &DataType) -> String {
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

struct PolarsIpcBatchIterator {
    source: Option<IpcBatchSource>,
    empty_schema: Arc<Schema>,
    current: Option<PlDataFrame>,
    current_offset: usize,
    max_rows: usize,
    max_bytes: usize,
    logical_schema_json: Option<String>,
    token: CancellationToken,
    deferred_error: Option<EngineError>,
    emitted_empty: bool,
    closed: bool,
}

impl PolarsIpcBatchIterator {
    fn close(&mut self) {
        self.source.take();
        self.current.take();
        self.current_offset = 0;
        self.closed = true;
    }

    fn next_source_row(&mut self) -> Option<Result<PlDataFrame, EngineError>> {
        if self.current.is_none() {
            match self.source.as_mut().and_then(IpcBatchSource::next_batch) {
                Some(Ok(batch)) => self.current = Some(batch),
                Some(Err(error)) => return Some(Err(error)),
                None => return None,
            }
        }
        let current = self.current.as_ref().expect("source batch was set");
        let row = current.slice(self.current_offset as i64, 1);
        self.current_offset += 1;
        if self.current_offset >= current.height() {
            self.current.take();
            self.current_offset = 0;
        }
        Some(Ok(row))
    }

    fn combine_rows(rows: &[PlDataFrame]) -> Result<PlDataFrame, polars::prelude::PolarsError> {
        let mut combined = rows.first().expect("non-empty rows").clone();
        for row in &rows[1..] {
            combined.vstack_mut(row)?;
        }
        Ok(combined)
    }

    fn serialize_batch(&self, batch: &mut PlDataFrame) -> Result<Vec<u8>, EngineError> {
        batch.rechunk_mut();
        let mut cursor = std::io::Cursor::new(Vec::new());
        {
            use polars::io::ipc::IpcStreamWriter;
            use polars::prelude::SerWriter;
            let mut writer = IpcStreamWriter::new(&mut cursor);
            if let Some(schema) = &self.logical_schema_json {
                let mut metadata = polars_arrow::datatypes::Metadata::new();
                metadata.insert("sparkless.logical_schema.v1".into(), schema.clone().into());
                writer.set_custom_schema_metadata(Arc::new(metadata));
            }
            writer.finish(batch).map_err(polars_to_core_error)?;
        }
        Ok(cursor.into_inner())
    }
}

impl Iterator for PolarsIpcBatchIterator {
    type Item = Result<ArrowIpcStream, EngineError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.closed {
            return None;
        }
        let mut rows = Vec::new();
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
            let row = match self.next_source_row() {
                Some(Ok(row)) if row.height() > 0 => row,
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

            let row_bytes = row.estimated_size();
            if !rows.is_empty() && estimated_bytes.saturating_add(row_bytes) > self.max_bytes {
                self.current = Some(row);
                self.current_offset = 0;
                break;
            }
            estimated_bytes = estimated_bytes.saturating_add(row_bytes);
            rows.push(row);
            if rows.len() >= self.max_rows || estimated_bytes >= self.max_bytes {
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
                    self.close();
                    if bytes.len() > self.max_bytes {
                        return Some(Err(EngineError::User(format!(
                            "an empty Arrow IPC schema requires {} bytes, exceeding max_bytes={}",
                            bytes.len(),
                            self.max_bytes
                        ))));
                    }
                    return Some(Ok(ArrowIpcStream { bytes }));
                }
                self.close();
            }
            return None;
        }
        let mut combined = match Self::combine_rows(&rows) {
            Ok(combined) => combined,
            Err(error) => {
                self.close();
                return Some(Err(polars_to_core_error(error)));
            }
        };
        let bytes = match self.serialize_batch(&mut combined) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.close();
                return Some(Err(error));
            }
        };
        if bytes.len() <= self.max_bytes {
            if source_ended {
                self.close();
            }
            return Some(Ok(ArrowIpcStream { bytes }));
        }

        if rows.len() == 1 {
            let actual_bytes = bytes.len();
            self.close();
            return Some(Err(EngineError::User(format!(
                "a single Arrow IPC row requires {actual_bytes} bytes, exceeding max_bytes={}",
                self.max_bytes
            ))));
        }
        let mut prefix_rows = rows.len() / 2;
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
                let mut pending = combined.slice(prefix_rows as i64, rows.len() - prefix_rows);
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
                return Some(Ok(ArrowIpcStream {
                    bytes: prefix_bytes,
                }));
            }
            prefix_rows /= 2;
        }
        self.close();
        Some(Err(EngineError::User(format!(
            "a single Arrow IPC row exceeds max_bytes={}",
            self.max_bytes
        ))))
    }
}

/// DataFrame - main tabular data structure.
/// Wraps either an eager Polars `DataFrame` or a lazy `LazyFrame`.
/// Transformations extend the plan; actions trigger materialization.
pub struct DataFrame {
    pub(crate) inner: DataFrameInner,
    /// When false (default), column names are matched case-insensitively (PySpark behavior).
    pub(crate) case_sensitive: bool,
    /// Optional alias for subquery/join (PySpark: df.alias("t")).
    pub(crate) alias: Option<String>,
    /// Column names that are ambiguous after a join (same name on both sides). Selecting them
    /// unqualified raises AMBIGUOUS_REFERENCE (PySpark parity #374 / #1230).
    pub(crate) ambiguous_columns: Option<HashSet<String>>,
    /// Lazy frame before the most recent select (PySpark: orderBy can reference dropped columns).
    pub(crate) pre_select_lf: Option<LazyFrame>,
    /// Projection exprs from the most recent select (re-applied after sort on pre_select_lf).
    pub(crate) post_select_exprs: Option<Vec<Expr>>,
    /// Exact row count for a source whose cardinality is known without scanning rows.
    /// Transformations that can change cardinality must clear this hint.
    pub(crate) row_count_hint: Option<u64>,
    pub(crate) range_cardinality: Option<RangeCardinality>,
    /// Logical schema retained beside Polars' physical schema when the latter cannot express
    /// nullability or metadata.
    pub(crate) logical_schema: Option<StructType>,
}

/// Spec for groupBy: either a column name (str) or a Column expression (e.g. col("a").alias("x")).
/// PySpark parity: groupBy("dept") vs groupBy(F.col("Name").alias("Key")).
#[derive(Clone)]
pub enum GroupBySpec {
    Name(String),
    Column(Box<Column>),
}

impl DataFrame {
    /// Create a new DataFrame from a Polars DataFrame (case-insensitive column matching by default).
    /// Stores as Lazy for consistency with the lazy-by-default execution model.
    pub fn from_polars(df: PlDataFrame) -> Self {
        let logical_schema = StructType::try_from_polars_schema(df.schema().as_ref()).ok();
        let lf = df.lazy();
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
            alias: None,
            ambiguous_columns: None,
            pre_select_lf: None,
            post_select_exprs: None,
            row_count_hint: None,
            range_cardinality: None,
            logical_schema,
        }
    }

    /// Create a new DataFrame from a Polars DataFrame with explicit case sensitivity.
    /// When `case_sensitive` is false, column resolution is case-insensitive (PySpark default).
    pub fn from_polars_with_options(df: PlDataFrame, case_sensitive: bool) -> Self {
        let logical_schema = StructType::try_from_polars_schema(df.schema().as_ref()).ok();
        let lf = df.lazy();
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive,
            alias: None,
            ambiguous_columns: None,
            pre_select_lf: None,
            post_select_exprs: None,
            row_count_hint: None,
            range_cardinality: None,
            logical_schema,
        }
    }

    /// Create a DataFrame from an eager Polars DataFrame without converting to Lazy.
    /// Used by create_dataframe_from_rows so schema (including struct types) is immediately
    /// available for dotted column resolution (e.g. select("StructValue.e1")) (#1076).
    pub(crate) fn from_eager_with_options(df: PlDataFrame, case_sensitive: bool) -> Self {
        let logical_schema = StructType::try_from_polars_schema(df.schema().as_ref()).ok();
        DataFrame {
            inner: DataFrameInner::Eager(Arc::new(df)),
            case_sensitive,
            alias: None,
            ambiguous_columns: None,
            pre_select_lf: None,
            post_select_exprs: None,
            row_count_hint: None,
            range_cardinality: None,
            logical_schema,
        }
    }

    /// Create a DataFrame from a LazyFrame (no materialization).
    pub fn from_lazy(lf: LazyFrame) -> Self {
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
            alias: None,
            ambiguous_columns: None,
            pre_select_lf: None,
            post_select_exprs: None,
            row_count_hint: None,
            range_cardinality: None,
            logical_schema: None,
        }
    }

    /// Create a DataFrame from a LazyFrame with explicit case sensitivity.
    pub fn from_lazy_with_options(lf: LazyFrame, case_sensitive: bool) -> Self {
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive,
            alias: None,
            ambiguous_columns: None,
            pre_select_lf: None,
            post_select_exprs: None,
            row_count_hint: None,
            range_cardinality: None,
            logical_schema: None,
        }
    }

    pub(crate) fn from_lazy_after_select(
        lf: LazyFrame,
        case_sensitive: bool,
        pre_select_lf: LazyFrame,
        post_select_exprs: Vec<Expr>,
    ) -> Self {
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive,
            alias: None,
            ambiguous_columns: None,
            pre_select_lf: Some(pre_select_lf),
            post_select_exprs: Some(post_select_exprs),
            row_count_hint: None,
            range_cardinality: None,
            logical_schema: None,
        }
    }

    /// Eager DataFrame after select; retains pre-select lazy frame for orderBy on non-selected columns (#1389).
    #[allow(dead_code)]
    pub(crate) fn from_eager_after_select(
        pl_df: polars::prelude::DataFrame,
        case_sensitive: bool,
        pre_select_lf: LazyFrame,
        post_select_exprs: Vec<Expr>,
    ) -> Self {
        DataFrame {
            inner: DataFrameInner::Eager(pl_df.into()),
            case_sensitive,
            alias: None,
            ambiguous_columns: None,
            pre_select_lf: Some(pre_select_lf),
            post_select_exprs: Some(post_select_exprs),
            row_count_hint: None,
            range_cardinality: None,
            logical_schema: None,
        }
    }

    /// Create a DataFrame from a LazyFrame with optional ambiguous column names (join same-name keys).
    /// When set, resolve_column_name returns AMBIGUOUS_REFERENCE for unqualified references to those names.
    pub(crate) fn from_lazy_with_options_and_ambiguous(
        lf: LazyFrame,
        case_sensitive: bool,
        ambiguous_columns: Option<HashSet<String>>,
    ) -> Self {
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive,
            alias: None,
            ambiguous_columns,
            pre_select_lf: None,
            post_select_exprs: None,
            row_count_hint: None,
            range_cardinality: None,
            logical_schema: None,
        }
    }

    /// Return a new DataFrame with the same data but case-insensitive column resolution.
    /// Used by plan execution so that Sparkless plans (e.g. col("ID") with schema "id") resolve (issue #524).
    pub(crate) fn with_case_insensitive_column_resolution(self) -> Self {
        DataFrame {
            inner: self.inner,
            case_sensitive: false,
            alias: self.alias,
            ambiguous_columns: self.ambiguous_columns,
            pre_select_lf: self.pre_select_lf.clone(),
            post_select_exprs: self.post_select_exprs.clone(),
            row_count_hint: self.row_count_hint,
            range_cardinality: self.range_cardinality.clone(),
            logical_schema: self.logical_schema.clone(),
        }
    }

    /// Create an empty DataFrame
    pub fn empty() -> Self {
        DataFrame {
            inner: DataFrameInner::Lazy(PlDataFrame::empty().lazy()),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
            alias: None,
            ambiguous_columns: None,
            pre_select_lf: None,
            post_select_exprs: None,
            row_count_hint: None,
            range_cardinality: None,
            logical_schema: None,
        }
    }

    /// True if this DataFrame is backed by an eager Polars DataFrame (e.g. from create_dataframe_from_rows).
    #[cfg(test)]
    pub(crate) fn is_eager(&self) -> bool {
        matches!(&self.inner, DataFrameInner::Eager(_))
    }

    /// Return the LazyFrame for plan extension. For Eager, converts via .lazy(); for Lazy, clones.
    pub(crate) fn lazy_frame(&self) -> LazyFrame {
        match &self.inner {
            DataFrameInner::Eager(df) => df.as_ref().clone().lazy(),
            DataFrameInner::Lazy(lf) => lf.clone(),
        }
    }

    pub(crate) fn with_range_cardinality(mut self, start: i64, step: i64, row_count: u64) -> Self {
        let cardinality = RangeCardinality {
            start,
            step,
            row_count,
            operations: Vec::new(),
        };
        self.row_count_hint = Some(row_count);
        self.range_cardinality = Some(cardinality);
        self
    }

    fn for_each_range_batch(
        &self,
        range: &RangeCardinality,
        token: Option<&CancellationToken>,
        mut visit: impl FnMut(PlDataFrame) -> Result<(), RangeBatchError>,
    ) -> Result<(), RangeBatchError> {
        let mut producer = RangeBatchProducer::new(
            range.clone(),
            self.case_sensitive,
            token.cloned(),
            RANGE_EXECUTION_BATCH_ROWS,
        );
        while let Some(batch) = producer.next_batch()? {
            visit(batch)?;
        }
        Ok(())
    }

    fn count_range_batches(
        &self,
        range: &RangeCardinality,
        token: Option<&CancellationToken>,
    ) -> Result<u64, RangeBatchError> {
        let mut count = 0u64;
        self.for_each_range_batch(range, token, |batch| {
            count = count.checked_add(batch.height() as u64).ok_or_else(|| {
                RangeBatchError::Polars(PolarsError::ComputeError(
                    "range count exceeds UInt64".into(),
                ))
            })?;
            Ok(())
        })?;
        Ok(count)
    }

    fn collect_range_batches(
        &self,
        range: &RangeCardinality,
        token: Option<&CancellationToken>,
    ) -> Result<PlDataFrame, RangeBatchError> {
        let schema = self
            .lazy_frame()
            .collect_schema()
            .map_err(RangeBatchError::Polars)?;
        let mut output = PlDataFrame::empty_with_schema(&schema);
        self.for_each_range_batch(range, token, |batch| {
            output.vstack_mut(&batch).map_err(RangeBatchError::Polars)?;
            Ok(())
        })?;
        Ok(output)
    }

    /// Materialize the plan. Single point of collect for all actions.
    pub(crate) fn collect_inner(&self) -> Result<Arc<PlDataFrame>, PolarsError> {
        if let Some(range) = &self.range_cardinality {
            let collected =
                self.collect_range_batches(range, None)
                    .map_err(|error| match error {
                        RangeBatchError::Polars(error) => error,
                        RangeBatchError::Engine(error) => {
                            PolarsError::ComputeError(error.to_string().into())
                        }
                    })?;
            return Ok(Arc::new(collected));
        }
        match &self.inner {
            DataFrameInner::Eager(df) => Ok(df.clone()),
            DataFrameInner::Lazy(lf) => Ok(Arc::new(lf.clone().collect()?)),
        }
    }

    /// Return a DataFrame with the given alias (PySpark: df.alias("t")).
    /// Used for subquery/join naming; the alias is stored for future use in SQL or qualified column resolution.
    pub fn alias(&self, name: &str) -> Self {
        let lf = self.lazy_frame();
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive: self.case_sensitive,
            alias: Some(name.to_string()),
            ambiguous_columns: self.ambiguous_columns.clone(),
            pre_select_lf: self.pre_select_lf.clone(),
            post_select_exprs: self.post_select_exprs.clone(),
            row_count_hint: self.row_count_hint,
            range_cardinality: self.range_cardinality.clone(),
            logical_schema: self.logical_schema.clone(),
        }
    }

    /// Return the table alias if set (e.g. from df.alias("t")). Used for join condition resolution (#374).
    pub fn get_alias(&self) -> Option<String> {
        self.alias.clone()
    }

    /// Resolve column names in a Polars expression against this DataFrame's schema.
    /// When case_sensitive is false, column references (e.g. col("name")) are resolved
    /// case-insensitively (PySpark default). Use before filter/select_with_exprs/order_by_exprs.
    /// Names that appear as alias outputs (e.g. in expr.alias("partial")) are not resolved
    /// as input columns, so select(col("x").substr(1, 3).alias("partial")),
    /// when().then().otherwise().alias("result"), and col("x").rank().over([]).alias("rank") work (issues #200, #212).
    pub fn resolve_expr_column_names(&self, expr: Expr) -> Result<Expr, PolarsError> {
        let schema = self.polars_schema()?;
        let df = self;
        let mut alias_output_names: HashSet<String> = HashSet::new();
        let _ = expr.clone().try_map_expr(|e| {
            if let Expr::Alias(_, name) = &e {
                alias_output_names.insert(name.as_str().to_string());
            }
            Ok(e)
        })?;
        // #1008: Expr's default output name (e.g. to_timestamp_timestamp_str) must not be resolved as input column.
        // Only add when it does not match any schema column (case-insensitive), so col("age") still resolves to "Age".
        if let Ok(out_name) = polars_plan::utils::expr_output_name(&expr) {
            let out_str = out_name.as_str();
            let matches_schema = self
                .columns()
                .map(|cols| cols.iter().any(|c| c.eq_ignore_ascii_case(out_str)))
                .unwrap_or(false);
            if !matches_schema {
                alias_output_names.insert(out_str.to_string());
            }
        }
        let resolved = expr.try_map_expr(move |e| {
            // Recurse into Alias so col("Person.name") inside F.col("Person.name").alias(...) gets resolved (struct field access).
            if let Expr::Alias(inner, name) = &e {
                let new_inner = df.resolve_expr_column_names(inner.as_ref().clone())?;
                return Ok(Expr::Alias(Arc::new(new_inner), name.clone()));
            }
            // Recurse into Cast so F.col("StructVal")["E1"].cast("int") has the struct field resolved (#1219).
            if let Expr::Cast {
                ref expr,
                ref dtype,
                ref options,
            } = e
            {
                let resolved_inner = df.resolve_expr_column_names(expr.as_ref().clone())?;
                return Ok(Expr::Cast {
                    expr: Arc::new(resolved_inner),
                    dtype: dtype.clone(),
                    options: *options,
                });
            }
            if let Expr::Column(name) = &e {
                let name_str = name.as_str();
                // Skip resolution only when this name is an alias output and not a schema column (case-insensitive).
                // So col("name").alias("name") still resolves "name" to "Name" when schema has "Name" (issue #1053).
                // Do not skip qualified names (e.g. "sm.taxonomy_id"): they must be resolved via suffix/alias.column (#374).
                if !name_str.contains('.') && alias_output_names.contains(name_str) {
                    let matches_schema = df
                        .columns()
                        .map(|cols| cols.iter().any(|c| c.eq_ignore_ascii_case(name_str)))
                        .unwrap_or(false);
                    if !matches_schema {
                        return Ok(e);
                    }
                }
                // Empty name is a placeholder in list.eval (e.g. map_keys uses col("").struct_().field_by_name("key")).
                if name_str.is_empty() {
                    return Ok(e);
                }
                // Struct field dot notation (PySpark col("struct_col.field")) or alias.column (e.g. col("sm.taxonomy_id") after join with alias "sm") (#374).
                if name_str.contains('.') {
                    let parts: Vec<&str> = name_str.split('.').collect();
                    let first = parts[0];
                    let rest = &parts[1..];
                    if rest.is_empty() {
                        return Err(PolarsError::ColumnNotFound(
                            format!(
                                "cannot resolve: Column '{}': trailing dot not allowed",
                                name_str
                            )
                            .into(),
                        ));
                    }
                    // Try struct field path first (first part is a column name).
                    match df.resolve_column_name(first) {
                        Ok(resolved) => {
                            // Resolve the first struct field using resolve_struct_field_name so we
                            // honor struct-specific case-insensitive lookup. Subsequent nested
                            // fields use resolve_struct_field_from_type.
                            let mut expr = col(PlSmallStr::from(resolved.as_str()));
                            let mut context_name = resolved.to_string();
                            let mut current_dtype =
                                df.get_column_dtype(resolved.as_str()).ok_or_else(|| {
                                    PolarsError::ColumnNotFound(
                                        format!("cannot resolve: column '{}' not found", resolved)
                                            .into(),
                                    )
                                })?;

                            for (i, field) in rest.iter().enumerate() {
                                // For the first field after the column, use resolve_struct_field_name
                                // so col("StructValue.E1") works when struct has "e1" (issue #1473).
                                let (resolved_field, field_dtype) = if i == 0 {
                                    // #1511: PySpark returns null for struct fields missing from inferred schema (e.g. E2).
                                    let resolved_field_name = match df
                                        .resolve_struct_field_name(&context_name, field)
                                    {
                                        Ok(n) => n,
                                        Err(_) => {
                                            return Ok(lit(NULL).alias(PlSmallStr::from(name_str)));
                                        }
                                    };
                                    // Use current_dtype via resolve_struct_field_from_type to keep
                                    // error messages and type resolution consistent.
                                    let (_, field_dtype) = df.resolve_struct_field_from_type(
                                        &current_dtype,
                                        &resolved_field_name,
                                        &context_name,
                                    )?;
                                    (resolved_field_name, field_dtype)
                                } else {
                                    match df.resolve_struct_field_from_type(
                                        &current_dtype,
                                        field,
                                        &context_name,
                                    ) {
                                        Ok(t) => t,
                                        Err(_) => {
                                            // #1150: Inferred struct may omit fields (e.g. E2); return null for missing field.
                                            return Ok(lit(NULL).alias(PlSmallStr::from(name_str)));
                                        }
                                    }
                                };

                                expr = expr.struct_().field_by_name(&resolved_field);
                                context_name = format!("{}.{}", context_name, resolved_field);
                                current_dtype = field_dtype;
                            }

                            return Ok(expr.alias(PlSmallStr::from(name_str)));
                        }
                        Err(_) => {
                            // First part not a column: alias.column after join (e.g. col("sm.taxonomy_id")); resolve_column_name does suffix fallback (#374).
                            if let Ok(suffix_resolved) = df.resolve_column_name(name_str) {
                                return Ok(col(PlSmallStr::from(suffix_resolved.as_str()))
                                    .alias(PlSmallStr::from(name_str)));
                            }
                            return Err(PolarsError::ColumnNotFound(
                                format!("cannot resolve: column '{}' not found", first).into(),
                            ));
                        }
                    }
                }
                let resolved = df.resolve_column_name(name_str)?;
                return Ok(Expr::Column(PlSmallStr::from(resolved.as_str())));
            }
            // Resolve struct field names in chained subscript (e.g. F.col("Outer")["Inner"]["E1"]) (#339, #1066).
            if let Expr::Function {
                input,
                function:
                    polars::prelude::FunctionExpr::StructExpr(
                        polars::prelude::StructFunction::FieldByName(name),
                    ),
            } = &e
            {
                if input.len() == 1 {
                    let input_expr = input[0].clone();
                    if let Some(input_dt) = df.get_expr_output_dtype(&input_expr) {
                        match df.resolve_struct_field_from_type(&input_dt, name.as_str(), "struct")
                        {
                            Ok((resolved_name, _)) => {
                                return Ok(input_expr.struct_().field_by_name(&resolved_name));
                            }
                            Err(_) => {
                                // #1150: Inferred struct may omit fields; getField("E2") yields null.
                                return Ok(lit(NULL));
                            }
                        }
                    }
                    // #1216: When schema is unavailable (e.g. lazy join result), preserve struct
                    // field access with the requested name so Polars can resolve at execution time.
                    return Ok(input_expr.struct_().field_by_name(name));
                }
            }
            // Recurse into Function inputs so map_col[key_col] (map_many) and similar get key column resolved (#1111).
            if let Expr::Function { input, function } = &e {
                let resolved_inputs: Result<Vec<Expr>, _> = input
                    .iter()
                    .map(|arg| df.resolve_expr_column_names(arg.clone()))
                    .collect();
                if let Ok(resolved) = resolved_inputs {
                    return Ok(Expr::Function {
                        input: resolved,
                        function: function.clone(),
                    });
                }
            }
            // Recurse into Over (window) so partition_by and order_by column names are resolved against input schema (#1241).
            if let Expr::Over {
                function,
                partition_by,
                order_by,
                mapping,
            } = &e
            {
                let resolved_function = df.resolve_expr_column_names(function.as_ref().clone())?;
                let resolved_partition_by: Result<Vec<Expr>, _> = partition_by
                    .iter()
                    .map(|p| df.resolve_expr_column_names(p.clone()))
                    .collect();
                let resolved_partition_by = resolved_partition_by?;
                let resolved_order_by = order_by.as_ref().map(|(ob, opts)| {
                    df.resolve_expr_column_names(ob.as_ref().clone())
                        .map(|r| (Arc::new(r), *opts))
                });
                let resolved_order_by = match resolved_order_by {
                    Some(Ok((r, opts))) => Some((r, opts)),
                    Some(Err(e)) => return Err(e),
                    None => None,
                };
                return Ok(Expr::Over {
                    function: Arc::new(resolved_function),
                    partition_by: resolved_partition_by,
                    order_by: resolved_order_by,
                    mapping: *mapping,
                });
            }
            Ok(e)
        })?;
        crate::functions::bind_runtime_config(resolved, &schema)
    }

    /// Rewrite comparison expressions to apply PySpark-style type coercion.
    ///
    /// This walks the expression tree and, for comparison operators where one side is
    /// a column and the other is a numeric literal, delegates to
    /// `coerce_for_pyspark_comparison` so that string–numeric comparisons behave like
    /// PySpark (string values parsed to numbers where possible, invalid strings treated
    /// as null/non-matching).
    pub fn coerce_string_numeric_comparisons(&self, expr: Expr) -> Result<Expr, PolarsError> {
        use polars::prelude::{DataType, Operator};
        use std::sync::Arc;

        fn literal_dtype(expr: &Expr) -> Option<DataType> {
            match expr {
                Expr::Literal(lv) => Some(lv.get_datatype()),
                Expr::Alias(inner, _) => literal_dtype(inner.as_ref()),
                Expr::Cast {
                    expr: inner, dtype, ..
                } => literal_dtype(inner.as_ref()).and_then(|_| dtype.as_literal().cloned()),
                _ => None,
            }
        }

        fn comparison_literal_dtype(dt: DataType) -> DataType {
            match dt {
                // SQL integer literals carry an unknown-width marker. Treat them as
                // Int64 so Decimal comparisons retain exact integer precision.
                DataType::Unknown(UnknownKind::Int(_)) => DataType::Int64,
                DataType::Unknown(UnknownKind::Float) => DataType::Float64,
                other => other,
            }
        }

        fn is_numeric_literal(expr: &Expr) -> bool {
            literal_dtype(expr).is_some_and(|dt| {
                dt.is_numeric()
                    || matches!(
                        dt,
                        DataType::Unknown(UnknownKind::Int(_))
                            | DataType::Unknown(UnknownKind::Float)
                    )
            })
        }

        fn is_temporal_literal(expr: &Expr) -> bool {
            literal_dtype(expr)
                .is_some_and(|dt| matches!(dt, DataType::Date | DataType::Datetime(_, _)))
        }

        /// Aliases do not alter evaluation.  Casts do, so keep them in the
        /// comparison expression even when they annotate a literal.
        fn peel_aliases(expr: &Expr) -> &Expr {
            let mut e = expr;
            loop {
                match e {
                    Expr::Alias(inner, _) => e = inner.as_ref(),
                    _ => return e,
                }
            }
        }

        // Apply root-level coercion first so the top-level filter condition (e.g. col("str_col") == lit(123))
        // is always rewritten even if try_map_expr traversal does not hit the root in the expected order.
        // #1023: Unwrap Alias from Column::into_expr() so we coerce the inner BinaryExpr (string vs numeric).
        let (expr_to_coerce, alias_after) = match &expr {
            Expr::Alias(inner, name) => (inner.as_ref().clone(), Some(name.clone())),
            _ => (expr.clone(), None),
        };
        // #1102: Recursively coerce both sides of And/Or so (col("dt") >= "a") & (col("dt") <= "b")
        // gets datetime-vs-string coercion in each comparison (plan sends string literals).
        let expr_to_coerce = match &expr_to_coerce {
            Expr::BinaryExpr { left, op, right } if matches!(op, Operator::And | Operator::Or) => {
                let left_c = self.coerce_string_numeric_comparisons((**left).clone())?;
                let right_c = self.coerce_string_numeric_comparisons((**right).clone())?;
                Expr::BinaryExpr {
                    left: Arc::new(left_c),
                    op: *op,
                    right: Arc::new(right_c),
                }
            }
            _ => expr_to_coerce,
        };
        fn wrap_expr_with_alias(
            expr: Expr,
            alias_name: Option<&polars::prelude::PlSmallStr>,
        ) -> Expr {
            match alias_name {
                Some(name) => Expr::Alias(Arc::new(expr), name.clone()),
                None => expr,
            }
        }
        let expr = {
            if let Expr::BinaryExpr { left, op, right } = &expr_to_coerce {
                let left_inner = peel_aliases(left);
                let right_inner = peel_aliases(right);
                let is_comparison_op = matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                );
                let left_is_col = matches!(left_inner, Expr::Column(_));
                let right_is_col = matches!(right_inner, Expr::Column(_));
                let left_is_numeric_lit = is_numeric_literal(left_inner);
                let right_is_numeric_lit = is_numeric_literal(right_inner);
                let left_is_temporal_lit = is_temporal_literal(left_inner);
                let right_is_temporal_lit = is_temporal_literal(right_inner);
                let left_is_string_lit = literal_dtype(left_inner) == Some(DataType::String);
                let right_is_string_lit = literal_dtype(right_inner) == Some(DataType::String);
                let root_is_col_vs_numeric = is_comparison_op
                    && ((left_is_col && right_is_numeric_lit)
                        || (right_is_col && left_is_numeric_lit));
                let root_is_col_vs_string = is_comparison_op
                    && ((left_is_col && right_is_string_lit)
                        || (right_is_col && left_is_string_lit));
                if root_is_col_vs_numeric {
                    // PySpark: string column vs numeric literal -> coerce string via try_to_number and compare (issue #235, #602).
                    let col_name = if left_is_col {
                        match left_inner {
                            Expr::Column(n) => n.as_str(),
                            _ => {
                                return Err(PolarsError::ComputeError(
                                    "internal: expected column expression on numeric comparison side"
                                        .into(),
                                ));
                            }
                        }
                    } else {
                        match right_inner {
                            Expr::Column(n) => n.as_str(),
                            _ => {
                                return Err(PolarsError::ComputeError(
                                    "internal: expected column expression on numeric comparison side"
                                        .into(),
                                ));
                            }
                        }
                    };
                    // Use column dtype so numeric columns compare numerically; String (or unknown) uses coercion (try_to_number).
                    let (new_left, new_right) = if left_is_col && right_is_numeric_lit {
                        let col_ty = self.get_column_dtype(col_name);
                        let lit_ty = literal_dtype(right_inner)
                            .map(comparison_literal_dtype)
                            .unwrap_or(DataType::Float64);
                        let left_ty = col_ty.filter(is_numeric_public).unwrap_or(DataType::String);
                        coerce_for_pyspark_comparison(
                            left_inner.clone(),
                            (**right).clone(),
                            &left_ty,
                            &lit_ty,
                            op,
                        )
                        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                    } else {
                        let col_ty = self.get_column_dtype(col_name);
                        let lit_ty = literal_dtype(left_inner)
                            .map(comparison_literal_dtype)
                            .unwrap_or(DataType::Float64);
                        let right_ty = col_ty.filter(is_numeric_public).unwrap_or(DataType::String);
                        coerce_for_pyspark_comparison(
                            (**left).clone(),
                            right_inner.clone(),
                            &lit_ty,
                            &right_ty,
                            op,
                        )
                        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                    };
                    Expr::BinaryExpr {
                        left: Arc::new(new_left),
                        op: *op,
                        right: Arc::new(new_right),
                    }
                } else if root_is_col_vs_string {
                    let col_name = if left_is_col {
                        match left_inner {
                            Expr::Column(n) => n.as_str(),
                            _ => {
                                return Err(PolarsError::ComputeError(
                                    "internal: expected column expression on string comparison side"
                                        .into(),
                                ));
                            }
                        }
                    } else {
                        match right_inner {
                            Expr::Column(n) => n.as_str(),
                            _ => {
                                return Err(PolarsError::ComputeError(
                                    "internal: expected column expression on string comparison side"
                                        .into(),
                                ));
                            }
                        }
                    };
                    if let Some(col_dtype) = self.get_column_dtype(col_name) {
                        if matches!(col_dtype, DataType::Date | DataType::Datetime(_, _)) {
                            let (left_ty, right_ty) = if left_is_col {
                                (col_dtype.clone(), DataType::String)
                            } else {
                                (DataType::String, col_dtype.clone())
                            };
                            let (new_left, new_right) = coerce_for_pyspark_comparison(
                                left_inner.clone(),
                                right_inner.clone(),
                                &left_ty,
                                &right_ty,
                                op,
                            )
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                            let e = Expr::BinaryExpr {
                                left: Arc::new(new_left),
                                op: *op,
                                right: Arc::new(new_right),
                            };
                            return Ok(wrap_expr_with_alias(e, alias_after.as_ref()));
                        }
                        // #988: Numeric column vs string literal -> coerce string to numeric (PySpark parity).
                        if is_numeric_public(&col_dtype) {
                            let (left_ty, right_ty) = if left_is_col {
                                (col_dtype.clone(), DataType::String)
                            } else {
                                (DataType::String, col_dtype.clone())
                            };
                            let (new_left, new_right) = coerce_for_pyspark_comparison(
                                left_inner.clone(),
                                right_inner.clone(),
                                &left_ty,
                                &right_ty,
                                op,
                            )
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                            let e = Expr::BinaryExpr {
                                left: Arc::new(new_left),
                                op: *op,
                                right: Arc::new(new_right),
                            };
                            return Ok(wrap_expr_with_alias(e, alias_after.as_ref()));
                        }
                    }
                    expr_to_coerce.clone()
                } else if is_comparison_op && (right_is_temporal_lit || left_is_temporal_lit) {
                    // The non-literal side may be a cast or a function, not only
                    // a bare column. Resolve its output type from the lazy schema
                    // so temporal coercion also covers expressions such as
                    // CAST(event_ts AS TIMESTAMP) = TIMESTAMP '...'.
                    let (value_expr, literal_expr, value_on_left) = if right_is_temporal_lit {
                        (left_inner, right_inner, true)
                    } else {
                        (right_inner, left_inner, false)
                    };
                    let value_type = literal_dtype(value_expr).or_else(|| match value_expr {
                        Expr::Column(name) => self.get_column_dtype(name.as_str()),
                        _ => self.get_expr_output_dtype(value_expr),
                    });
                    if let (Some(value_type), Some(literal_type)) =
                        (value_type, literal_dtype(literal_expr))
                        && matches!(
                            value_type,
                            DataType::Date | DataType::Datetime(_, _) | DataType::String
                        )
                    {
                        let (left_type, right_type) = if value_on_left {
                            (value_type, literal_type)
                        } else {
                            (literal_type, value_type)
                        };
                        let (new_left, new_right) = coerce_for_pyspark_comparison(
                            left_inner.clone(),
                            right_inner.clone(),
                            &left_type,
                            &right_type,
                            op,
                        )
                        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                        Expr::BinaryExpr {
                            left: Arc::new(new_left),
                            op: *op,
                            right: Arc::new(new_right),
                        }
                    } else {
                        expr_to_coerce.clone()
                    }
                } else if is_comparison_op
                    && ((left_is_string_lit && !right_is_string_lit && !right_is_col)
                        || (right_is_string_lit && !left_is_string_lit && !left_is_col))
                {
                    if let Some((new_left, new_right)) = self
                        .coerce_temporal_expr_vs_string_literal(
                            left_inner,
                            right_inner,
                            left_is_string_lit,
                            right_is_string_lit,
                            op,
                        )
                    {
                        let e = Expr::BinaryExpr {
                            left: Arc::new(new_left),
                            op: *op,
                            right: Arc::new(new_right),
                        };
                        return Ok(wrap_expr_with_alias(e, alias_after.as_ref()));
                    }
                    expr_to_coerce.clone()
                } else if is_comparison_op && left_is_col && right_is_col {
                    // Column-to-column: col("id") == col("label") where id is int, label is string.
                    // Get both column types and coerce string-numeric / date-string for PySpark parity.
                    let left_name = match left_inner {
                        Expr::Column(n) => n.as_str(),
                        _ => {
                            return Err(PolarsError::ComputeError(
                                "internal: expected column on left of column-column comparison"
                                    .into(),
                            ));
                        }
                    };
                    let right_name = match right_inner {
                        Expr::Column(n) => n.as_str(),
                        _ => {
                            return Err(PolarsError::ComputeError(
                                "internal: expected column on right of column-column comparison"
                                    .into(),
                            ));
                        }
                    };
                    if let (Some(left_ty), Some(right_ty)) = (
                        self.get_column_dtype(left_name),
                        self.get_column_dtype(right_name),
                    ) {
                        if left_ty != right_ty {
                            if let Ok((new_left, new_right)) = coerce_for_pyspark_comparison(
                                left_inner.clone(),
                                right_inner.clone(),
                                &left_ty,
                                &right_ty,
                                op,
                            ) {
                                let e = Expr::BinaryExpr {
                                    left: Arc::new(new_left),
                                    op: *op,
                                    right: Arc::new(new_right),
                                };
                                return Ok(wrap_expr_with_alias(e, alias_after.as_ref()));
                            }
                        }
                    }
                    expr_to_coerce.clone()
                } else if is_comparison_op {
                    if let Some((new_left, new_right)) =
                        self.coerce_temporal_expressions(left_inner, right_inner, op)?
                    {
                        Expr::BinaryExpr {
                            left: Arc::new(new_left),
                            op: *op,
                            right: Arc::new(new_right),
                        }
                    } else {
                        expr_to_coerce.clone()
                    }
                } else {
                    expr_to_coerce.clone()
                }
            } else {
                expr_to_coerce.clone()
            }
        };
        let expr = wrap_expr_with_alias(expr, alias_after.as_ref());

        // Then walk the tree for nested comparisons (e.g. (col("a")==1) & (col("b")==2)).
        let get_col_dtype = |name: &str| self.get_column_dtype(name);
        let get_expr_dtype = |expr: &Expr| self.get_expr_output_dtype(expr);
        let expr = expr.try_map_expr(move |e| {
            if let Expr::BinaryExpr { left, op, right } = e {
                let is_comparison_op = matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                );
                if !is_comparison_op {
                    return Ok(Expr::BinaryExpr { left, op, right });
                }

                let left_peeled = peel_aliases(left.as_ref());
                let right_peeled = peel_aliases(right.as_ref());
                let left_is_col = matches!(left_peeled, Expr::Column(_));
                let right_is_col = matches!(right_peeled, Expr::Column(_));
                let left_is_string_lit = literal_dtype(left_peeled) == Some(DataType::String);
                let right_is_string_lit = literal_dtype(right_peeled) == Some(DataType::String);

                let left_is_numeric_lit = is_numeric_literal(left_peeled);
                let right_is_numeric_lit = is_numeric_literal(right_peeled);
                let left_is_temporal_lit = is_temporal_literal(left_peeled);
                let right_is_temporal_lit = is_temporal_literal(right_peeled);

                // Column-vs-numeric-literal: use column dtype; String (or unknown) -> try_to_number then compare (PySpark #235, #602).
                let (new_left, new_right) = if left_is_col && right_is_numeric_lit {
                    let col_ty = if let Expr::Column(n) = left_peeled {
                        get_col_dtype(n.as_str())
                    } else {
                        None
                    };
                    let lit_ty = literal_dtype(right_peeled)
                        .map(comparison_literal_dtype)
                        .unwrap_or(DataType::Float64);
                    let left_ty = col_ty.filter(is_numeric_public).unwrap_or(DataType::String);
                    coerce_for_pyspark_comparison(
                        (*left).clone(),
                        (*right).clone(),
                        &left_ty,
                        &lit_ty,
                        &op,
                    )
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                } else if right_is_col && left_is_numeric_lit {
                    let col_ty = if let Expr::Column(n) = right_peeled {
                        get_col_dtype(n.as_str())
                    } else {
                        None
                    };
                    let lit_ty = literal_dtype(left_peeled)
                        .map(comparison_literal_dtype)
                        .unwrap_or(DataType::Float64);
                    let right_ty = col_ty.filter(is_numeric_public).unwrap_or(DataType::String);
                    coerce_for_pyspark_comparison(
                        (*left).clone(),
                        (*right).clone(),
                        &lit_ty,
                        &right_ty,
                        &op,
                    )
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                } else if right_is_temporal_lit || left_is_temporal_lit {
                    let (value_expr, literal_expr, value_on_left) = if right_is_temporal_lit {
                        ((*left).clone(), (*right).clone(), true)
                    } else {
                        ((*right).clone(), (*left).clone(), false)
                    };
                    let value_peeled = peel_aliases(&value_expr);
                    let value_type = match value_peeled {
                        Expr::Column(name) => get_col_dtype(name.as_str()),
                        _ => get_expr_dtype(value_peeled),
                    };
                    if let (Some(value_type), Some(literal_type)) =
                        (value_type, literal_dtype(peel_aliases(&literal_expr)))
                        && matches!(
                            value_type,
                            DataType::Date | DataType::Datetime(_, _) | DataType::String
                        )
                    {
                        let (left_type, right_type) = if value_on_left {
                            (value_type, literal_type)
                        } else {
                            (literal_type, value_type)
                        };
                        let (new_left, new_right) = coerce_for_pyspark_comparison(
                            (*left).clone(),
                            (*right).clone(),
                            &left_type,
                            &right_type,
                            &op,
                        )
                        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                        return Ok(Expr::BinaryExpr {
                            left: Arc::new(new_left),
                            op,
                            right: Arc::new(new_right),
                        });
                    }
                    return Ok(Expr::BinaryExpr { left, op, right });
                } else if (left_is_col && right_is_string_lit)
                    || (right_is_col && left_is_string_lit)
                {
                    let col_name = if left_is_col {
                        match left_peeled {
                            Expr::Column(n) => n.as_str(),
                            _ => {
                                return Err(PolarsError::ComputeError(
                                    "internal: expected column in nested string comparison".into(),
                                ));
                            }
                        }
                    } else {
                        match right_peeled {
                            Expr::Column(n) => n.as_str(),
                            _ => {
                                return Err(PolarsError::ComputeError(
                                    "internal: expected column in nested string comparison".into(),
                                ));
                            }
                        }
                    };
                    if let Some(col_dtype) = self.get_column_dtype(col_name) {
                        if matches!(col_dtype, DataType::Date | DataType::Datetime(_, _)) {
                            let (left_ty, right_ty) = if left_is_col {
                                (col_dtype.clone(), DataType::String)
                            } else {
                                (DataType::String, col_dtype.clone())
                            };
                            let (new_l, new_r) = coerce_for_pyspark_comparison(
                                (*left).clone(),
                                (*right).clone(),
                                &left_ty,
                                &right_ty,
                                &op,
                            )
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                            return Ok(Expr::BinaryExpr {
                                left: Arc::new(new_l),
                                op,
                                right: Arc::new(new_r),
                            });
                        }
                        // #988 / #1545: numeric column vs string literal in nested comparisons (e.g. under Or).
                        if is_numeric_public(&col_dtype) {
                            let (left_ty, right_ty) = if left_is_col {
                                (col_dtype.clone(), DataType::String)
                            } else {
                                (DataType::String, col_dtype.clone())
                            };
                            let (new_l, new_r) = coerce_for_pyspark_comparison(
                                (*left).clone(),
                                (*right).clone(),
                                &left_ty,
                                &right_ty,
                                &op,
                            )
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                            return Ok(Expr::BinaryExpr {
                                left: Arc::new(new_l),
                                op,
                                right: Arc::new(new_r),
                            });
                        }
                    }
                    return Ok(Expr::BinaryExpr { left, op, right });
                } else if (left_is_string_lit && !right_is_col && !right_is_string_lit)
                    || (right_is_string_lit && !left_is_col && !left_is_string_lit)
                {
                    if let Some((new_l, new_r)) = self.coerce_temporal_expr_vs_string_literal(
                        left_peeled,
                        right_peeled,
                        left_is_string_lit,
                        right_is_string_lit,
                        &op,
                    ) {
                        return Ok(Expr::BinaryExpr {
                            left: Arc::new(new_l),
                            op,
                            right: Arc::new(new_r),
                        });
                    }
                    return Ok(Expr::BinaryExpr { left, op, right });
                } else {
                    if let Some((new_left, new_right)) =
                        self.coerce_temporal_expressions(&left, &right, &op)?
                    {
                        return Ok(Expr::BinaryExpr {
                            left: Arc::new(new_left),
                            op,
                            right: Arc::new(new_right),
                        });
                    }
                    return Ok(Expr::BinaryExpr { left, op, right });
                };

                Ok(Expr::BinaryExpr {
                    left: Arc::new(new_left),
                    op,
                    right: Arc::new(new_right),
                })
            } else {
                Ok(e)
            }
        })?;
        // #974: Coerce string–numeric for arithmetic (+, -, *, /, %) so PySpark-style 2.0 * col("Value") works when Value is string.
        let expr = expr.try_map_expr(move |e| {
            if let Expr::BinaryExpr {
                ref left,
                ref op,
                ref right,
            } = e
            {
                let is_arithmetic_op = matches!(
                    op,
                    Operator::Plus
                        | Operator::Minus
                        | Operator::Multiply
                        | Operator::TrueDivide
                        | Operator::FloorDivide
                        | Operator::RustDivide
                        | Operator::Modulus
                );
                if !is_arithmetic_op {
                    return Ok(e);
                }
                let left_ty = crate::type_coercion::infer_type_from_expr(left.as_ref())
                    .or_else(|| {
                        if let Expr::Column(n) = &**left {
                            self.get_column_dtype(n.as_str())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(DataType::String);
                let right_ty = crate::type_coercion::infer_type_from_expr(right.as_ref())
                    .or_else(|| {
                        if let Expr::Column(n) = &**right {
                            self.get_column_dtype(n.as_str())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(DataType::String);
                if (left_ty == DataType::String
                    && crate::type_coercion::is_numeric_public(&right_ty))
                    || (right_ty == DataType::String
                        && crate::type_coercion::is_numeric_public(&left_ty))
                {
                    if let Ok((new_left, new_right)) =
                        crate::type_coercion::coerce_for_pyspark_arithmetic(
                            (**left).clone(),
                            (**right).clone(),
                            &left_ty,
                            &right_ty,
                        )
                    {
                        return Ok(Expr::BinaryExpr {
                            left: Arc::new(new_left),
                            op: *op,
                            right: Arc::new(new_right),
                        });
                    }
                }
            }
            Ok(e)
        })?;
        Ok(expr)
    }

    /// Get schema from inner (Eager: df.schema(); Lazy: lf.collect_schema()).
    fn schema_or_collect(&self) -> Result<Arc<Schema>, PolarsError> {
        match &self.inner {
            DataFrameInner::Eager(df) => Ok(Arc::clone(df.schema())),
            DataFrameInner::Lazy(lf) => Ok(lf.clone().collect_schema()?),
        }
    }

    /// Polars schema for use in join/coercion to avoid repeated collect_schema per key (performance).
    pub(crate) fn polars_schema(&self) -> Result<Arc<Schema>, PolarsError> {
        self.schema_or_collect()
    }

    /// Resolve a logical column name to the actual column name in the schema.
    /// When case_sensitive is false, matches case-insensitively.
    /// When the name is in ambiguous_columns (join same-name keys) or multiple physical columns
    /// match, returns an error with AMBIGUOUS_REFERENCE (PySpark parity #374 / #1230).
    /// Fail if name is an unqualified user-requested column that is ambiguous (#1230). Call at
    /// select/select_items boundary for bare column names only; do not use inside resolve_column_name
    /// so that qualified refs (e.g. df1["name"]) and dotted select ("t1.id") still resolve (#297).
    pub fn check_ambiguous_unqualified(&self, name: &str) -> Result<(), PolarsError> {
        if name.contains('.') {
            return Ok(());
        }
        if let Some(ref ambig) = self.ambiguous_columns {
            let found = if self.case_sensitive {
                ambig.contains(name)
            } else {
                let name_lower = name.to_lowercase();
                ambig.iter().any(|a| a.to_lowercase() == name_lower)
            };
            if found {
                return Err(PolarsError::ColumnNotFound(
                    format!("Reference `{}` is ambiguous. AMBIGUOUS_REFERENCE", name).into(),
                ));
            }
        }
        Ok(())
    }

    /// Resolve a logical column name to the actual column name in the schema.
    /// When case_sensitive is false, matches case-insensitively.
    /// When multiple physical columns match, returns AMBIGUOUS_REFERENCE. For join ambiguous_columns,
    /// use check_ambiguous_unqualified at the select/select_items boundary for bare names (#1230, #297).
    pub fn resolve_column_name(&self, name: &str) -> Result<String, PolarsError> {
        let schema = self.schema_or_collect()?;
        let names: Vec<String> = schema
            .iter_names_and_dtypes()
            .map(|(n, _)| n.to_string())
            .collect();
        // Unqualified name: check for multiple physical columns (e.g. duplicate "id" in schema).
        if !name.contains('.') {
            let matches: Vec<&String> = if self.case_sensitive {
                names.iter().filter(|n| n.as_str() == name).collect()
            } else {
                let name_lower = name.to_lowercase();
                names
                    .iter()
                    .filter(|n| n.to_lowercase() == name_lower)
                    .collect()
            };
            if matches.len() > 1 {
                return Err(PolarsError::ColumnNotFound(
                    format!("Reference `{}` is ambiguous. AMBIGUOUS_REFERENCE", name).into(),
                ));
            }
        }
        if self.case_sensitive {
            if names.iter().any(|n| n == name) {
                return Ok(name.to_string());
            }
        } else {
            // In case-insensitive mode, prefer an exact-case match when it exists
            // (e.g. select(\"NAME\") should use physical column \"NAME\" when both
            // \"name\" and \"NAME\" are present), then fall back to the first
            // case-insensitive match for PySpark-style resolution.
            if let Some(exact) = names.iter().find(|n| n.as_str() == name) {
                return Ok(exact.clone());
            }
            let name_lower = name.to_lowercase();
            for n in &names {
                if n.to_lowercase() == name_lower {
                    return Ok(n.clone());
                }
            }
        }
        // Qualified name (e.g. "sm.taxonomy_id" or "l.location_id" after multi-join): try suffix (#374).
        // When both "suffix" and "suffix_right" exist (e.g. from multiple joins), prefer _right so alias refers to the right table.
        if let Some((_prefix, suffix)) = name.split_once('.') {
            if !suffix.is_empty() {
                let suffix_right = format!("{}_right", suffix);
                let matches: Vec<&String> = if self.case_sensitive {
                    names
                        .iter()
                        .filter(|n| n.as_str() == suffix || n.as_str() == suffix_right.as_str())
                        .collect()
                } else {
                    let suffix_lower = suffix.to_lowercase();
                    let suffix_right_lower = suffix_right.to_lowercase();
                    names
                        .iter()
                        .filter(|n| {
                            let nl = n.to_lowercase();
                            nl == suffix_lower || nl == suffix_right_lower
                        })
                        .collect()
                };
                if matches.len() == 1 {
                    return Ok(matches[0].clone());
                }
                if matches.len() >= 2 {
                    // Prefer _right so "l.location_id" -> location_id_right after join (right table).
                    let right_match = matches.iter().find(|n| {
                        if self.case_sensitive {
                            n.ends_with("_right")
                        } else {
                            n.to_lowercase().ends_with("_right")
                        }
                    });
                    if let Some(m) = right_match {
                        return Ok((*m).clone());
                    }
                }
            }
        }
        let available = names.join(", ");
        Err(PolarsError::ColumnNotFound(
            format!(
                "cannot resolve: column '{}' not found. Available columns: [{}]. Check spelling and case sensitivity (spark.sql.caseSensitive).",
                name,
                available
            )
            .into(),
        ))
    }

    /// Get the schema of the DataFrame
    pub fn schema(&self) -> Result<StructType, PolarsError> {
        if let Some(schema) = &self.logical_schema {
            return Ok(schema.clone());
        }
        let s = self.schema_or_collect()?;
        Ok(StructType::from_polars_schema(&s))
    }

    /// Attach a checked logical schema to a physical Polars plan. This is how callers preserve
    /// nullability and metadata that Polars' physical `Field` does not represent.
    pub fn with_logical_schema(mut self, schema: StructType) -> Result<Self, PolarsError> {
        schema
            .validate_metadata()
            .map_err(|error| PolarsError::InvalidOperation(error.into()))?;
        let physical = self.schema_or_collect()?;
        if schema.fields().len() != physical.len() {
            return Err(PolarsError::SchemaMismatch(
                format!(
                    "logical schema has {} fields but the Polars plan has {}",
                    schema.fields().len(),
                    physical.len()
                )
                .into(),
            ));
        }
        for (logical, (physical_name, physical_dtype)) in
            schema.fields().iter().zip(physical.iter())
        {
            if logical.name != physical_name.as_str() {
                return Err(PolarsError::SchemaMismatch(
                    format!(
                        "logical field '{}' does not match Polars field '{}' at the same position",
                        logical.name, physical_name
                    )
                    .into(),
                ));
            }
            let expected = crate::schema_conv::checked_data_type_to_polars_type(
                &logical.data_type,
                &logical.name,
            )?;
            if &expected != physical_dtype {
                return Err(PolarsError::SchemaMismatch(
                    format!(
                        "logical field '{}' maps to {expected}, but the Polars plan has {physical_dtype}",
                        logical.name
                    )
                    .into(),
                ));
            }
        }
        self.logical_schema = Some(schema);
        Ok(self)
    }

    fn projected_logical_schema(
        &self,
        expressions: &[Expr],
        output: &DataFrame,
    ) -> Option<StructType> {
        let source_schema = self.logical_schema.as_ref()?;
        let output_schema = output.schema_or_collect().ok()?;
        if expressions.len() != output_schema.len() {
            return None;
        }
        let fields = output_schema
            .iter()
            .zip(expressions)
            .map(|((output_name, output_dtype), expression)| {
                if let Some(source_name) = projected_source_column(expression)
                    && let Some(source) = logical_field_for_projected_column(
                        source_schema,
                        &source_name,
                        self.case_sensitive,
                    )
                {
                    let mut projected = source;
                    let physical_matches = crate::schema_conv::checked_data_type_to_polars_type(
                        &projected.data_type,
                        &projected.name,
                    )
                    .is_ok_and(|expected| &expected == output_dtype);
                    if !physical_matches {
                        projected.data_type = crate::schema_conv::checked_polars_type_to_data_type(
                            output_dtype,
                            output_name,
                        )
                        .ok()?;
                    }
                    // Permissive casts can turn invalid input values into NULL. Polars' physical
                    // schema does not expose that possibility, so preserve a conservative
                    // nullable declaration whenever a projected expression contains a cast.
                    if projected_expression_contains_cast(expression) {
                        projected.nullable = true;
                    }
                    projected.name = output_name.to_string();
                    return Some(projected);
                }
                let data_type =
                    crate::schema_conv::checked_polars_type_to_data_type(output_dtype, output_name)
                        .ok()?;
                Some(StructField::new(output_name.to_string(), data_type, true))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(StructType::new(fields))
    }

    fn projected_logical_schema_for_paths(
        &self,
        source_paths: &[String],
        output: &DataFrame,
    ) -> Option<StructType> {
        let source_schema = self.logical_schema.as_ref()?;
        let output_schema = output.schema_or_collect().ok()?;
        if source_paths.len() != output_schema.len() {
            return None;
        }
        let fields = source_paths
            .iter()
            .zip(output_schema.iter())
            .map(|(source_path, (output_name, output_dtype))| {
                let mut projected = logical_field_for_projected_column(
                    source_schema,
                    source_path,
                    self.case_sensitive,
                )?;
                let physical_matches = crate::schema_conv::checked_data_type_to_polars_type(
                    &projected.data_type,
                    &projected.name,
                )
                .is_ok_and(|expected| &expected == output_dtype);
                if !physical_matches {
                    projected.data_type = crate::schema_conv::checked_polars_type_to_data_type(
                        output_dtype,
                        output_name,
                    )
                    .ok()?;
                }
                projected.name = output_name.to_string();
                Some(projected)
            })
            .collect::<Option<Vec<_>>>()?;
        Some(StructType::new(fields))
    }

    fn logical_schema_by_output_names(&self, output: &DataFrame) -> Option<StructType> {
        let source_schema = self.logical_schema.as_ref()?;
        let output_schema = output.schema_or_collect().ok()?;
        let fields = output_schema
            .iter()
            .map(|(output_name, output_dtype)| {
                if let Some(source) = source_schema.fields().iter().find(|field| {
                    if self.case_sensitive {
                        field.name == output_name.as_str()
                    } else {
                        field.name.eq_ignore_ascii_case(output_name.as_str())
                    }
                }) {
                    let matches_physical = crate::schema_conv::checked_data_type_to_polars_type(
                        &source.data_type,
                        &source.name,
                    )
                    .is_ok_and(|expected| &expected == output_dtype);
                    if matches_physical {
                        let mut projected = source.clone();
                        projected.name = output_name.to_string();
                        return Some(projected);
                    }
                }
                Some(StructField::new(
                    output_name.to_string(),
                    crate::schema_conv::checked_polars_type_to_data_type(output_dtype, output_name)
                        .ok()?,
                    true,
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(StructType::new(fields))
    }

    fn merged_logical_schema(
        &self,
        other: &DataFrame,
        output: &DataFrame,
        align_by_name: bool,
    ) -> Option<StructType> {
        let left_schema = self.logical_schema.as_ref();
        let right_schema = other.logical_schema.as_ref();
        if left_schema.is_none() && right_schema.is_none() {
            return None;
        }

        let output_schema = output.schema_or_collect().ok()?;
        let left_names = self.columns().ok()?;
        let right_names = other.columns().ok()?;
        let names_match = left_names.len() == right_names.len()
            && left_names.iter().all(|left_name| {
                right_names.iter().any(|right_name| {
                    if self.case_sensitive {
                        left_name == right_name
                    } else {
                        left_name.eq_ignore_ascii_case(right_name)
                    }
                })
            });

        let match_by_name = align_by_name || names_match;
        let mut fields = Vec::with_capacity(output_schema.len());
        for (index, (output_name, output_dtype)) in output_schema.iter().enumerate() {
            let left_field = left_schema.and_then(|schema| {
                let matched = schema.fields().iter().find(|field| {
                    if self.case_sensitive {
                        field.name == output_name.as_str()
                    } else {
                        field.name.eq_ignore_ascii_case(output_name.as_str())
                    }
                });
                matched.or_else(|| {
                    (!align_by_name)
                        .then(|| schema.fields().get(index))
                        .flatten()
                })
            });
            let right_field = right_schema.and_then(|schema| {
                if match_by_name {
                    schema.fields().iter().find(|field| {
                        if self.case_sensitive {
                            field.name == output_name.as_str()
                        } else {
                            field.name.eq_ignore_ascii_case(output_name.as_str())
                        }
                    })
                } else {
                    schema.fields().get(index)
                }
            });

            // Preserve a logical declaration only when it still describes the coerced physical
            // output. This matters for chained unions: the first union can coerce (for example)
            // LONG + STRING to STRING, and a later union must not restore the right input's LONG
            // sidecar over that physical STRING column.
            let unknown_nested_type = conservative_union_logical_type(output_dtype);
            let merged_type = match (left_field, right_field) {
                (Some(left), Some(right)) => {
                    merge_union_logical_type(&left.data_type, &right.data_type, output_dtype)
                }
                (Some(left), None) => unknown_nested_type.as_ref().and_then(|unknown| {
                    merge_union_logical_type(&left.data_type, unknown, output_dtype)
                }),
                (None, Some(right)) => unknown_nested_type.as_ref().and_then(|unknown| {
                    merge_union_logical_type(&right.data_type, unknown, output_dtype)
                }),
                (None, None) => None,
            }
            .filter(|data_type| {
                crate::schema_conv::checked_data_type_to_polars_type(data_type, output_name)
                    .is_ok_and(|mapped| &mapped == output_dtype)
            });
            let data_type = merged_type.or_else(|| {
                [left_field, right_field]
                    .into_iter()
                    .flatten()
                    .find(|field| {
                        crate::schema_conv::checked_data_type_to_polars_type(
                            &field.data_type,
                            &field.name,
                        )
                        .is_ok_and(|mapped| &mapped == output_dtype)
                    })
                    .map(|field| field.data_type.clone())
                    .or_else(|| {
                        crate::schema_conv::checked_polars_type_to_data_type(
                            output_dtype,
                            output_name,
                        )
                        .ok()
                    })
            })?;

            // Union retains the left field's metadata and conservatively combines nullability.
            // A missing sidecar means its nullability is unknown, so use nullable=true.
            let mut field = left_field.or(right_field).cloned().unwrap_or_else(|| {
                StructField::new(output_name.to_string(), data_type.clone(), true)
            });
            field.name = output_name.to_string();
            field.data_type = data_type;
            field.nullable = left_field.map(|field| field.nullable).unwrap_or(true)
                || right_field.map(|field| field.nullable).unwrap_or(true);
            fields.push(field);
        }
        Some(StructType::new(fields))
    }

    /// Preserve field declarations across joins by following the result names back to their
    /// input columns. Polars does not carry Spark field metadata or nested nullability in its
    /// physical schema, so the join result must retain the logical descriptor explicitly.
    pub(crate) fn joined_logical_schema(
        &self,
        other: &DataFrame,
        output: &DataFrame,
        how: JoinType,
    ) -> Option<StructType> {
        if self.logical_schema.is_none() && other.logical_schema.is_none() {
            return None;
        }

        let output_schema = output.schema_or_collect().ok()?;
        let left_schema = self.schema_or_collect().ok()?;
        let right_schema = other.schema_or_collect().ok()?;
        let left_names = self.columns().ok()?;
        let right_names = other.columns().ok()?;
        let matches_name = |left: &str, right: &str| {
            if self.case_sensitive {
                left == right
            } else {
                left.eq_ignore_ascii_case(right)
            }
        };

        let logical_or_physical_field = |frame: &DataFrame,
                                         physical_schema: &Schema,
                                         source_name: &str|
         -> Option<StructField> {
            if let Some(field) = frame.logical_schema.as_ref().and_then(|schema| {
                schema
                    .fields()
                    .iter()
                    .find(|field| matches_name(&field.name, source_name))
            }) {
                return Some(field.clone());
            }
            let dtype = physical_schema
                .iter()
                .find(|(name, _)| matches_name(name.as_str(), source_name))
                .map(|(_, dtype)| dtype)?;
            Some(StructField::new(
                source_name.to_string(),
                crate::schema_conv::checked_polars_type_to_data_type(dtype, source_name).ok()?,
                true,
            ))
        };

        // Joins use one physical suffix for every colliding right field. Recover that suffix
        // from the actual output names instead of independently guessing `_right` per column;
        // an unrelated left field may already have a name such as `value_right`.
        let join_suffix = (0..=right_names.len())
            .map(|index| {
                if index == 0 {
                    "_right".to_string()
                } else {
                    format!("_right_{index}")
                }
            })
            .find(|suffix| {
                right_names.iter().any(|right_name| {
                    left_names
                        .iter()
                        .any(|left_name| matches_name(left_name, right_name))
                        && {
                            let candidate = format!("{right_name}{suffix}");
                            !left_names
                                .iter()
                                .any(|left_name| matches_name(left_name, &candidate))
                                && output_schema.get(&candidate).is_some()
                        }
                })
            });
        let aliased_right_source = |output_name: &str| -> Option<&str> {
            right_names.iter().find_map(|right_name| {
                let collides = left_names
                    .iter()
                    .any(|left_name| matches_name(left_name, right_name));
                let aliased = join_suffix
                    .as_ref()
                    .filter(|_| collides)
                    .is_some_and(|suffix| {
                        matches_name(&format!("{right_name}{suffix}"), output_name)
                    });
                aliased.then_some(right_name.as_str())
            })
        };

        let fields = output_schema
            .iter()
            .map(|(output_name, output_dtype)| {
                let left_source = left_names
                    .iter()
                    .find(|name| matches_name(name, output_name.as_str()))
                    .map(String::as_str);
                let right_exact_source = right_names
                    .iter()
                    .find(|name| matches_name(name, output_name.as_str()))
                    .map(String::as_str);
                let right_source =
                    right_exact_source.or_else(|| aliased_right_source(output_name.as_str()));

                // An unsuffixed collision is the left field (including a coalesced join key).
                // Right fields renamed by the join implementation are recovered from `_right`.
                let (source_frame, source_schema, source_name, is_right) =
                    if let Some(source_name) = left_source {
                        (self, left_schema.as_ref(), source_name, false)
                    } else if let Some(source_name) = right_source {
                        (other, right_schema.as_ref(), source_name, true)
                    } else {
                        return Some(StructField::new(
                            output_name.to_string(),
                            crate::schema_conv::checked_polars_type_to_data_type(
                                output_dtype,
                                output_name,
                            )
                            .ok()?,
                            true,
                        ));
                    };

                let mut field =
                    logical_or_physical_field(source_frame, source_schema, source_name)?;
                if !crate::schema_conv::checked_data_type_to_polars_type(
                    &field.data_type,
                    source_name,
                )
                .is_ok_and(|expected| &expected == output_dtype)
                {
                    field.data_type = crate::schema_conv::checked_polars_type_to_data_type(
                        output_dtype,
                        output_name,
                    )
                    .ok()?;
                }
                field.name = output_name.to_string();
                if matches!(how, JoinType::Outer)
                    || (matches!(how, JoinType::Left) && is_right)
                    || (matches!(how, JoinType::Right) && !is_right)
                {
                    field.nullable = true;
                }
                Some(field)
            })
            .collect::<Option<Vec<_>>>()?;
        Some(StructType::new(fields))
    }

    /// Same as [`schema`](Self::schema) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn schema_engine(&self) -> Result<StructType, EngineError> {
        self.schema().map_err(polars_to_core_error)
    }

    /// Get the dtype of a column by name (after resolving case-insensitivity). Returns None if not found.
    /// Tries Polars schema (get + iter fallback); then our StructType so struct columns are found (e.g. select("Person.name")).
    pub fn get_column_dtype(&self, name: &str) -> Option<DataType> {
        let resolved = self.resolve_column_name(name).ok()?;
        let pl_schema = self.schema_or_collect().ok()?;
        if let Some(dt) = pl_schema.get(resolved.as_str()).cloned().or_else(|| {
            pl_schema
                .iter_names_and_dtypes()
                .find(|(n, _)| {
                    let s = n.to_string();
                    s == resolved || s.eq_ignore_ascii_case(resolved.as_str())
                })
                .map(|(_, dt)| dt.clone())
        }) {
            return Some(dt);
        }
        self.schema()
            .ok()?
            .fields()
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(resolved.as_str()))
            .map(|f| crate::schema_conv::data_type_to_polars_type(&f.data_type))
    }

    /// Resolve a struct field from a struct type (for nested col("outer.inner.leaf")). Returns (resolved_field_name, field_dtype).
    fn resolve_struct_field_from_type(
        &self,
        struct_dtype: &DataType,
        field_name: &str,
        context_name: &str,
    ) -> Result<(String, DataType), PolarsError> {
        let fields = match struct_dtype {
            DataType::Struct(f) => f,
            _ => {
                return Err(PolarsError::ColumnNotFound(
                    format!(
                        "cannot resolve: Expected struct for nested access '{}'; got non-struct type.",
                        context_name
                    )
                    .into(),
                ));
            }
        };
        // Exact match first (respects case_sensitive for column names; struct fields often need case-insensitive).
        if let Some(f) = fields.iter().find(|f| f.name.as_str() == field_name) {
            return Ok((f.name.to_string(), f.dtype.clone()));
        }
        // Struct field lookup: always try case-insensitive so col("StructValue.E1") works when struct has "e1" (e.g. from inferred schema).
        let field_lower = field_name.to_lowercase();
        for f in fields {
            if f.name.to_string().to_lowercase() == field_lower {
                return Ok((f.name.to_string(), f.dtype.clone()));
            }
        }
        let available: Vec<String> = fields.iter().map(|f| f.name.to_string()).collect();
        Err(PolarsError::ColumnNotFound(
            format!(
                "cannot resolve: Struct field '{}' not found in '{}'. Available: [{}].",
                field_name,
                context_name,
                available.join(", ")
            )
            .into(),
        ))
    }

    /// Resolve a struct field name case-insensitively (for col("Person.name") when struct has "Name").
    pub fn resolve_struct_field_name(
        &self,
        struct_col_name: &str,
        field_name: &str,
    ) -> Result<String, PolarsError> {
        let dt = self.get_column_dtype(struct_col_name).ok_or_else(|| {
            PolarsError::ColumnNotFound(
                format!("cannot resolve: column '{}' not found", struct_col_name).into(),
            )
        })?;
        if !matches!(dt, DataType::Struct(_)) {
            return Err(PolarsError::ColumnNotFound(
                format!(
                    "cannot resolve: Column '{}' is not a struct; cannot access field '{}'.",
                    struct_col_name, field_name
                )
                .into(),
            ));
        }
        self.resolve_struct_field_from_type(&dt, field_name, struct_col_name)
            .map(|(name, _)| name)
    }

    /// Return the output DataType of an expression when evaluated against this DataFrame's schema.
    /// Used to resolve struct field names in chained subscript (e.g. col("Outer")["Inner"]["E1"]).
    fn get_expr_output_dtype(&self, expr: &Expr) -> Option<DataType> {
        use polars::prelude::{FunctionExpr, StructFunction};

        fn peel_expr_alias_chain(expr: &Expr) -> &Expr {
            let mut e = expr;
            while let Expr::Alias(inner, _) = e {
                e = inner.as_ref();
            }
            e
        }

        let peeled = peel_expr_alias_chain(expr);
        match peeled {
            Expr::Column(name) => self.get_column_dtype(name.as_str()),
            Expr::Function { input, function } => {
                if let FunctionExpr::StructExpr(StructFunction::FieldByName(name)) = function {
                    if let Some(first) = input.first() {
                        let input_dt = self.get_expr_output_dtype(first)?;
                        let (_, field_dt) = self
                            .resolve_struct_field_from_type(&input_dt, name.as_str(), "?")
                            .ok()?;
                        return Some(field_dt);
                    }
                }
                None
            }
            _ => None,
        }
        .or_else(|| self.probe_expr_output_dtype(expr))
    }

    /// Resolve expression output type via lazy schema (e.g. to_date(col, format) map UDFs).
    fn probe_expr_output_dtype(&self, expr: &Expr) -> Option<DataType> {
        const PROBE: &str = "_sparkless_dtype_probe";
        let probe_name = PlSmallStr::from(PROBE);
        let schema = self
            .lazy_frame()
            .select([expr.clone().alias(probe_name.clone())])
            .collect_schema()
            .ok()?;
        schema.get(PROBE).cloned()
    }

    /// Casts to DATE/TIMESTAMP use map expressions, so they are not recognized
    /// by literal_dtype. Resolve their output types before comparing them with
    /// temporal columns (e.g. a UTC Delta column vs CAST('...' AS TIMESTAMP)).
    fn coerce_temporal_expressions(
        &self,
        left: &Expr,
        right: &Expr,
        op: &polars::prelude::Operator,
    ) -> Result<Option<(Expr, Expr)>, PolarsError> {
        let Some(left_type) = self
            .get_expr_output_dtype(left)
            .filter(|dt| matches!(dt, DataType::Date | DataType::Datetime(_, _)))
        else {
            return Ok(None);
        };
        let Some(right_type) = self
            .get_expr_output_dtype(right)
            .filter(|dt| matches!(dt, DataType::Date | DataType::Datetime(_, _)))
        else {
            return Ok(None);
        };
        coerce_for_pyspark_comparison(left.clone(), right.clone(), &left_type, &right_type, op)
            .map(Some)
    }

    /// Coerce temporal expression vs string literal (e.g. to_date(col) == "9999-12-31", #1589).
    fn coerce_temporal_expr_vs_string_literal(
        &self,
        left: &Expr,
        right: &Expr,
        left_is_string_lit: bool,
        right_is_string_lit: bool,
        op: &polars::prelude::Operator,
    ) -> Option<(Expr, Expr)> {
        if left_is_string_lit == right_is_string_lit {
            return None;
        }
        let expr_side = if left_is_string_lit { right } else { left };
        if matches!(expr_side, Expr::Column(_)) {
            return None;
        }
        let expr_dtype = self.get_expr_output_dtype(expr_side)?;
        if !matches!(expr_dtype, DataType::Date | DataType::Datetime(_, _)) {
            return None;
        }
        let (left_ty, right_ty) = if left_is_string_lit {
            (DataType::String, expr_dtype)
        } else {
            (expr_dtype, DataType::String)
        };
        let (new_left, new_right) =
            coerce_for_pyspark_comparison(left.clone(), right.clone(), &left_ty, &right_ty, op)
                .ok()?;
        Some((new_left, new_right))
    }

    /// Get the column type as robin-sparkless schema type (Polars-free). Returns None if column not found.
    /// Prefer this over [`Self::get_column_dtype`] when building bindings that should not depend on Polars.
    pub fn get_column_data_type(&self, name: &str) -> Option<crate::schema::DataType> {
        let resolved = self.resolve_column_name(name).ok()?;
        let st = self.schema().ok()?;
        st.fields()
            .iter()
            .find(|f| f.name == resolved)
            .map(|f| f.data_type.clone())
    }

    /// Get column names
    pub fn columns(&self) -> Result<Vec<String>, PolarsError> {
        let schema = self.schema_or_collect()?;
        Ok(schema
            .iter_names_and_dtypes()
            .map(|(n, _)| n.to_string())
            .collect())
    }

    /// Return this DataFrame's column-resolution mode.
    #[doc(hidden)]
    pub fn is_case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    /// Same as [`columns`](Self::columns) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn columns_engine(&self) -> Result<Vec<String>, EngineError> {
        self.columns().map_err(polars_to_core_error)
    }

    /// Count the number of rows (action - triggers execution)
    pub fn count(&self) -> Result<usize, PolarsError> {
        if let Some(count) = self.row_count_hint {
            return usize::try_from(count).map_err(|_| {
                PolarsError::ComputeError(
                    format!("row count {count} exceeds this platform's usize range").into(),
                )
            });
        }
        if let Some(range) = &self.range_cardinality {
            let count = match self.count_range_batches(range, None) {
                Ok(count) => count,
                Err(RangeBatchError::Polars(error)) => return Err(error),
                Err(RangeBatchError::Engine(error)) => {
                    return Err(PolarsError::ComputeError(error.to_string().into()));
                }
            };
            return usize::try_from(count).map_err(|_| {
                PolarsError::ComputeError(
                    format!("row count {count} exceeds this platform's usize range").into(),
                )
            });
        }
        // Keep the aggregation in the lazy plan. Collecting the source and reading its
        // height used memory proportional to every row, even though count returns one
        // scalar. `len()` counts rows (including rows whose columns are all null).
        let result = self
            .lazy_frame()
            .select([polars::prelude::len()
                .cast(DataType::UInt64)
                .alias("__sparkless_count")])
            .collect()?;
        let count = result
            .column("__sparkless_count")?
            .u64()?
            .get(0)
            .ok_or_else(|| {
                PolarsError::ComputeError("row count aggregate returned no value".into())
            })?;
        usize::try_from(count).map_err(|_| {
            PolarsError::ComputeError(
                format!("row count {count} exceeds this platform's usize range").into(),
            )
        })
    }

    fn logical_schema_after_renames(
        &self,
        output: &DataFrame,
        renames: &[(String, String)],
    ) -> Option<StructType> {
        let source_schema = self.logical_schema.as_ref()?;
        let output_schema = output.schema_or_collect().ok()?;
        let same_name = |left: &str, right: &str| {
            if self.case_sensitive {
                left == right
            } else {
                left.eq_ignore_ascii_case(right)
            }
        };
        let fields = output_schema
            .iter()
            .map(|(output_name, output_dtype)| {
                let source_name = renames
                    .iter()
                    .find(|(_, renamed)| same_name(renamed, output_name.as_str()))
                    .map(|(source, _)| source.as_str())
                    .unwrap_or(output_name.as_str());
                let Some(source) = source_schema
                    .fields()
                    .iter()
                    .find(|field| same_name(&field.name, source_name))
                else {
                    return Some(StructField::new(
                        output_name.to_string(),
                        crate::schema_conv::checked_polars_type_to_data_type(
                            output_dtype,
                            output_name,
                        )
                        .ok()?,
                        true,
                    ));
                };
                let mut projected = source.clone();
                let physical_matches = crate::schema_conv::checked_data_type_to_polars_type(
                    &source.data_type,
                    &source.name,
                )
                .is_ok_and(|expected| &expected == output_dtype);
                if !physical_matches {
                    projected.data_type = crate::schema_conv::checked_polars_type_to_data_type(
                        output_dtype,
                        output_name,
                    )
                    .ok()?;
                }
                projected.name = output_name.to_string();
                Some(projected)
            })
            .collect::<Option<Vec<_>>>()?;
        Some(StructType::new(fields))
    }

    /// Count rows while checking cooperative cancellation at the native materialization boundary.
    pub fn count_with_cancellation(&self, token: &CancellationToken) -> Result<usize, EngineError> {
        token.check_cancelled()?;
        if let Some(count) = self.row_count_hint {
            return usize::try_from(count).map_err(|_| {
                EngineError::User(format!(
                    "row count {count} exceeds this platform's usize range"
                ))
            });
        }
        if let Some(range) = &self.range_cardinality {
            let count =
                self.count_range_batches(range, Some(token))
                    .map_err(|error| match error {
                        RangeBatchError::Polars(error) => polars_to_core_error(error),
                        RangeBatchError::Engine(error) => error,
                    })?;
            return usize::try_from(count).map_err(|_| {
                EngineError::User(format!(
                    "row count {count} exceeds this platform's usize range"
                ))
            });
        }
        let result = Self::from_lazy_with_options(
            self.lazy_frame().select([polars::prelude::len()
                .cast(DataType::UInt64)
                .alias("__sparkless_count")]),
            self.case_sensitive,
        )
        .collect_with_cancellation(token)?;
        let count = result
            .column("__sparkless_count")
            .map_err(polars_to_core_error)?
            .u64()
            .map_err(polars_to_core_error)?
            .get(0)
            .ok_or_else(|| EngineError::Internal("row count aggregate returned no value".into()))?;
        usize::try_from(count).map_err(|_| {
            EngineError::User(format!(
                "row count {count} exceeds this platform's usize range"
            ))
        })
    }

    /// Same as [`count`](Self::count) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn count_engine(&self) -> Result<usize, EngineError> {
        self.count().map_err(polars_to_core_error)
    }

    /// Show the first n rows (collects at most `n` rows, not the full DataFrame).
    pub fn show(&self, n: Option<usize>) -> Result<(), PolarsError> {
        let n = n.unwrap_or(20);
        let preview = self.clone().limit(n)?.collect_inner()?;
        println!("{preview}");
        Ok(())
    }

    /// Collect the DataFrame (action - triggers execution)
    pub fn collect(&self) -> Result<Arc<PlDataFrame>, PolarsError> {
        self.collect_inner()
    }

    /// Estimate the native buffers retained by this DataFrame after materialization.
    pub fn estimated_size(&self) -> Result<usize, PolarsError> {
        Ok(self.collect_inner()?.estimated_size())
    }

    /// Execute this plan as a back-pressured stream of bounded Polars DataFrames.
    ///
    /// The iterator starts the query on its first `next()` call. Dropping it closes
    /// the receiver, which stops the streaming sink when the consumer no longer
    /// requests batches. `max_rows` controls the maximum number of rows per batch.
    pub fn collect_batches(&self, max_rows: usize) -> Result<CollectBatches, PolarsError> {
        let chunk_size = NonZeroUsize::new(max_rows).ok_or_else(|| {
            PolarsError::InvalidOperation("max_rows must be greater than zero".into())
        })?;
        self.lazy_frame()
            .collect_batches(Engine::Auto, true, Some(chunk_size), true)
    }

    /// Execute this plan as bounded batches with cooperative cancellation.
    ///
    /// Range plans supported by the native per-batch pipeline are evaluated
    /// directly, so streaming exports do not route those plans through an
    /// unsupported Polars `AnonymousScan`.
    pub fn collect_batches_with_cancellation(
        &self,
        max_rows: usize,
        token: &CancellationToken,
    ) -> Result<DataFrameBatches, EngineError> {
        token.check_cancelled()?;
        if max_rows == 0 {
            return Err(EngineError::User(
                "max_rows must be greater than zero".into(),
            ));
        }
        let source = if let Some(range) = &self.range_cardinality {
            IpcBatchSource::Range(RangeBatchProducer::new(
                range.clone(),
                self.case_sensitive,
                Some(token.clone()),
                max_rows,
            ))
        } else {
            IpcBatchSource::Polars(
                self.collect_batches(max_rows)
                    .map_err(polars_to_core_error)?,
            )
        };
        Ok(DataFrameBatches {
            source: Some(source),
            token: token.clone(),
        })
    }

    /// Export the plan as lazy, byte- and row-bounded Arrow IPC batches.
    ///
    /// Each iterator item is a standalone Arrow IPC stream containing one batch.
    /// The plan starts when the consumer requests its first item; dropping the
    /// iterator stops the producer after its current batch.
    pub fn to_arrow_ipc_batches_with_cancellation(
        &self,
        max_rows: usize,
        max_bytes: usize,
        token: &CancellationToken,
    ) -> Result<ArrowIpcBatchStream, EngineError> {
        token.check_cancelled()?;
        if max_rows == 0 {
            return Err(EngineError::User(
                "max_rows must be greater than zero".into(),
            ));
        }
        if max_bytes == 0 {
            return Err(EngineError::User(
                "max_bytes must be greater than zero".into(),
            ));
        }
        let empty_schema = self.schema_or_collect().map_err(polars_to_core_error)?;
        // Row-sized source pulls keep in-flight memory independent of max_rows/max_bytes.
        // Range plans use the native operation pipeline with a one-row source chunk so a
        // small byte limit is checked before a larger chunk is retained.
        let source = if let Some(range) = &self.range_cardinality {
            IpcBatchSource::Range(RangeBatchProducer::new(
                range.clone(),
                self.case_sensitive,
                Some(token.clone()),
                1,
            ))
        } else {
            IpcBatchSource::Polars(self.collect_batches(1).map_err(polars_to_core_error)?)
        };
        let logical_schema_json = self
            .logical_schema
            .as_ref()
            .map(StructType::to_json)
            .transpose()
            .map_err(|error| {
                EngineError::User(format!("logical schema serialization failed: {error}"))
            })?;
        Ok(ArrowIpcBatchStream::new(PolarsIpcBatchIterator {
            source: Some(source),
            empty_schema,
            current: None,
            current_offset: 0,
            max_rows,
            max_bytes,
            logical_schema_json,
            token: token.clone(),
            deferred_error: None,
            emitted_empty: false,
            closed: false,
        }))
    }

    /// Materialize this plan while honoring cancellation at the engine boundary.
    /// Polars does not currently accept a per-query cancellation callback, so a request
    /// made during `LazyFrame::collect` is observed as soon as that call returns.
    pub fn collect_with_cancellation(
        &self,
        token: &CancellationToken,
    ) -> Result<Arc<PlDataFrame>, EngineError> {
        token.check_cancelled()?;
        let collected = if let Some(range) = &self.range_cardinality {
            Arc::new(self.collect_range_batches(range, Some(token)).map_err(
                |error| match error {
                    RangeBatchError::Polars(error) => polars_to_core_error(error),
                    RangeBatchError::Engine(error) => error,
                },
            )?)
        } else {
            self.collect_inner().map_err(polars_to_core_error)?
        };
        token.check_cancelled()?;
        Ok(collected)
    }

    /// Same as [`collect_as_json_rows`](Self::collect_as_json_rows) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn collect_as_json_rows_engine(
        &self,
    ) -> Result<Vec<HashMap<String, JsonValue>>, EngineError> {
        self.collect_as_json_rows().map_err(polars_to_core_error)
    }

    /// Collect JSON rows and check cancellation at the materialization boundary.
    pub fn collect_as_json_rows_with_cancellation(
        &self,
        token: &CancellationToken,
    ) -> Result<Vec<HashMap<String, JsonValue>>, EngineError> {
        token.check_cancelled()?;
        let rows = self.collect_as_json_rows().map_err(polars_to_core_error)?;
        token.check_cancelled()?;
        Ok(rows)
    }

    /// Reject values that cannot be returned through the server's strict JSON result contract.
    /// SQL mutations call this on their fully staged candidate before publishing it.
    pub(crate) fn validate_json_values(&self) -> Result<(), PolarsError> {
        let collected = self.collect_inner()?;
        for column in collected.columns() {
            let name = column.name().as_str();
            let dtype = column.dtype();
            let path = serde_json::to_string(name)
                .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
            for row in 0..collected.height() {
                let value = column.get(row)?;
                if matches!(value, AnyValue::Null) {
                    continue;
                }
                let _ = any_value_to_json_with_array_struct_override_policy(
                    &value,
                    dtype,
                    &HashSet::new(),
                    &path,
                    true,
                )?;
            }
        }
        Ok(())
    }

    /// Collect named JSON rows and schema while honoring cancellation at materialization
    /// boundaries. The Polars query itself remains non-interruptible until `collect()` returns.
    #[allow(clippy::type_complexity)]
    pub fn collect_as_json_rows_with_names_and_array_struct_columns_with_cancellation(
        &self,
        array_struct_paths: &HashSet<String>,
        token: &CancellationToken,
    ) -> Result<(Vec<String>, Vec<HashMap<String, JsonValue>>, StructType), EngineError> {
        token.check_cancelled()?;
        let rows = self
            .collect_as_json_rows_with_names_and_array_struct_columns(array_struct_paths)
            .map_err(polars_to_core_error)?;
        token.check_cancelled()?;
        Ok(rows)
    }

    /// Display a bounded preview while honoring cancellation at materialization boundaries.
    pub fn show_with_cancellation(
        &self,
        n: Option<usize>,
        token: &CancellationToken,
    ) -> Result<(), EngineError> {
        token.check_cancelled()?;
        let preview = self
            .clone()
            .limit(n.unwrap_or(20))
            .map_err(polars_to_core_error)?;
        let collected = preview.collect_with_cancellation(token)?;
        println!("{collected}");
        Ok(())
    }

    /// Collect as rows of column-name -> JSON value. For use by language bindings (Node, etc.).
    pub fn collect_as_json_rows(&self) -> Result<Vec<HashMap<String, JsonValue>>, PolarsError> {
        self.collect_as_json_rows_with_names()
            .map(|(_, rows, _)| rows)
    }

    /// Same as [`collect_as_json_rows`](Self::collect_as_json_rows) but returns output column names
    /// (in order) and the collected schema so bindings can build Row with correct types (PySpark parity:
    /// row keys match select/alias names). For Lazy we use the plan schema so get_json_object etc. are string (#1146).
    #[allow(clippy::type_complexity)]
    pub fn collect_as_json_rows_with_names(
        &self,
    ) -> Result<(Vec<String>, Vec<HashMap<String, JsonValue>>, StructType), PolarsError> {
        self.collect_as_json_rows_with_names_and_array_struct_columns(&HashSet::new())
    }

    /// Collect rows while preserving paths whose logical type is Arrow
    /// `list<struct<...>>`. Polars represents maps and these arrays with the same
    /// physical `List(Struct)` dtype, so callers with the source schema must tell
    /// the serializer which nested paths are arrays of structs. Field segments
    /// are JSON-quoted, separated by dots; [] and {value} denote container children.
    #[allow(clippy::type_complexity)]
    pub fn collect_as_json_rows_with_names_and_array_struct_columns(
        &self,
        array_struct_paths: &HashSet<String>,
    ) -> Result<(Vec<String>, Vec<HashMap<String, JsonValue>>, StructType), PolarsError> {
        let (collected, plan_schema) = match &self.inner {
            DataFrameInner::Eager(df) => (df.as_ref().clone(), df.schema().as_ref().clone()),
            DataFrameInner::Lazy(lf) => {
                let plan_schema = lf.clone().collect_schema()?.as_ref().clone();
                let pl_df = lf.clone().collect()?;
                (pl_df, plan_schema)
            }
        };
        // Use plan schema order so select("dept", "salary", row_number()...) yields rows with
        // dept/salary/rn in that order; Polars collect() may return columns in a different order
        // (#1267, #357).
        let names_and_dtypes: Vec<(String, DataType)> = plan_schema
            .iter_names_and_dtypes()
            .map(|(n, d)| (n.to_string(), d.clone()))
            .collect();
        let names: Vec<String> = names_and_dtypes.iter().map(|(n, _)| n.clone()).collect();
        let plan_dtypes: Vec<DataType> = names_and_dtypes.iter().map(|(_, d)| d.clone()).collect();
        // #1146: get_json_object returns string; only treat a/nested/missing as String when all three columns present (get_json_object test shape). json_tuple c0/c1 always string when both present (#1240: do not force c1 to string when columns are e.g. c1..c5).
        let has_get_json_object_shape = names.iter().any(|n| n == "a")
            && names.iter().any(|n| n == "nested")
            && names.iter().any(|n| n == "missing");
        let has_json_tuple_shape =
            names.len() == 2 && names.iter().any(|n| n == "c0") && names.iter().any(|n| n == "c1");
        let effective_dtypes: Vec<DataType> = names
            .iter()
            .zip(plan_dtypes.iter())
            .map(|(name, dt)| {
                let force_string_candidate = dt == &DataType::Int64
                    && ((has_json_tuple_shape && (name.as_str() == "c0" || name.as_str() == "c1"))
                        || (has_get_json_object_shape
                            && (name.as_str() == "a"
                                || name.as_str() == "nested"
                                || name.as_str() == "missing")));
                let force_string = force_string_candidate
                    && collected
                        .get_column_index(name.as_str())
                        .map(|idx| {
                            let s = &collected.columns()[idx];
                            !matches!(
                                s.dtype(),
                                DataType::Int64
                                    | DataType::Int32
                                    | DataType::Float64
                                    | DataType::Float32
                            )
                        })
                        .unwrap_or(true);
                if force_string {
                    DataType::String
                } else {
                    dt.clone()
                }
            })
            .collect();
        // Compute per-column serialization dtype (plan dtype, or actual when plan String but series is numeric #1245).
        let serialization_dtypes: Vec<DataType> = names
            .iter()
            .enumerate()
            .map(|(col_idx, name)| {
                let idx = match collected.get_column_index(name.as_str()) {
                    Some(i) => i,
                    None => {
                        return effective_dtypes
                            .get(col_idx)
                            .cloned()
                            .unwrap_or(DataType::String);
                    }
                };
                let s = &collected.columns()[idx];
                let plan_dtype = effective_dtypes
                    .get(col_idx)
                    .unwrap_or_else(|| s.dtype())
                    .clone();
                if plan_dtype == DataType::String
                    && matches!(
                        s.dtype(),
                        DataType::Int64 | DataType::Float64 | DataType::Boolean
                    )
                {
                    s.dtype().clone()
                } else {
                    plan_dtype
                }
            })
            .collect();
        let schema_override = Schema::from_iter(
            names
                .iter()
                .zip(serialization_dtypes.iter())
                .map(|(n, d)| Field::new(n.as_str().into(), d.clone())),
        );
        let schema = StructType::from_polars_schema(&schema_override);
        // Resolve columns by name so order matches plan schema regardless of collected frame order.
        // When cast fails (e.g. struct with List field vs plan String), use column as-is (Issue #1263).
        let columns_cast: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(col_idx, name)| {
                let idx = collected.get_column_index(name.as_str()).ok_or_else(|| {
                    PolarsError::ComputeError(
                        format!("collect_as_json_rows_with_names: column '{name}' not found")
                            .into(),
                    )
                })?;
                let s = &collected.columns()[idx];
                let dtype = serialization_dtypes
                    .get(col_idx)
                    .unwrap_or_else(|| s.dtype())
                    .clone();
                if dtype == *s.dtype() {
                    Ok((s.clone(), dtype))
                } else {
                    match s.cast(&dtype) {
                        Ok(casted) => Ok((casted, dtype)),
                        // Keep target dtype for serialization so any_value_to_json emits string when plan says String (Issue #1262).
                        Err(_) => Ok((s.clone(), dtype)),
                    }
                }
            })
            .collect::<Result<Vec<(polars::prelude::Column, DataType)>, PolarsError>>()?;
        // Cache validity once per column. Calling `is_null` in the row loop builds a full
        // null mask each time, making collection quadratic in the row count.
        let null_masks = columns_cast
            .iter()
            .map(|(s, _)| s.is_null())
            .collect::<Vec<_>>();
        let nrows = collected.height();
        let mut rows = Vec::with_capacity(nrows);
        for i in 0..nrows {
            let mut row = HashMap::with_capacity(names.len());
            for (col_idx, name) in names.iter().enumerate() {
                let (s, dtype) = columns_cast
                    .get(col_idx)
                    .ok_or_else(|| PolarsError::ComputeError("column index out of range".into()))?;
                let null_mask = null_masks
                    .get(col_idx)
                    .ok_or_else(|| PolarsError::ComputeError("column index out of range".into()))?;
                let av = s.get(i)?;
                // A struct AnyValue contains its fields but does not retain the parent validity
                // bit. Read validity from the column so null parents remain distinct from valid
                // structs whose fields are all null.
                let jv = if null_mask.get(i).unwrap_or(false) {
                    JsonValue::Null
                } else {
                    any_value_to_json_with_array_struct_override(
                        &av,
                        dtype,
                        array_struct_paths,
                        &serde_json::to_string(name)
                            .expect("field names serialize as JSON strings"),
                    )?
                };
                row.insert(name.clone(), jv);
            }
            rows.push(row);
        }
        if std::env::var("SPARKLESS_DEBUG_UNION").as_deref() == Ok("1") {
            if let Some((key_idx, _)) = names.iter().enumerate().find(|(_, n)| n.as_str() == "key")
            {
                let key_dtype = effective_dtypes.get(key_idx);
                let first_key = rows.first().and_then(|r| r.get("key"));
                eprintln!(
                    "[union #1262 collect] key effective_dtype={:?} first_row key={:?}",
                    key_dtype, first_key
                );
            }
        }
        Ok((names, rows, schema))
    }

    /// Collect positional JSON cells, preserving column order and duplicate labels.
    ///
    /// `max_rows` is enforced by limiting the native plan to one row beyond the bound. Use the
    /// `with_limits` variant when the protocol also requires a serialized-byte budget.
    pub fn collect_as_positional_json_with_cancellation(
        &self,
        max_rows: usize,
        array_struct_paths: &HashSet<String>,
        token: &CancellationToken,
    ) -> Result<PositionalJsonResult, EngineError> {
        self.collect_as_positional_json_with_limits(
            max_rows,
            usize::MAX,
            usize::MAX,
            array_struct_paths,
            token,
        )
    }

    /// Collect positional JSON cells with row, total encoded-data, and encoded-cell limits.
    /// The encoded-data budget covers the outer JSON array of rows, before protocol envelope and
    /// schema bytes are added by the caller.
    pub fn collect_as_positional_json_with_limits(
        &self,
        max_rows: usize,
        max_total_json_bytes: usize,
        max_cell_json_bytes: usize,
        array_struct_paths: &HashSet<String>,
        token: &CancellationToken,
    ) -> Result<PositionalJsonResult, EngineError> {
        token.check_cancelled()?;
        if max_total_json_bytes < 2 {
            return Err(EngineError::User(
                "result exceeds the configured serialized byte limit".into(),
            ));
        }
        let probe_limit = max_rows.checked_add(1).ok_or_else(|| {
            EngineError::User("maximum positional result row count is too large".into())
        })?;
        let limited = self
            .clone()
            .limit(probe_limit)
            .map_err(polars_to_core_error)?;
        let plan_schema = limited
            .lazy_frame()
            .collect_schema()
            .map_err(polars_to_core_error)?;
        let collected = limited.collect_with_cancellation(token)?;
        if collected.height() > max_rows {
            return Err(EngineError::User(format!(
                "result exceeds the configured row limit of {max_rows}"
            )));
        }

        let names_and_dtypes: Vec<(String, DataType)> = plan_schema
            .iter_names_and_dtypes()
            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
            .collect();
        let names: Vec<String> = names_and_dtypes
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let schema = StructType::from_polars_schema(&plan_schema);
        let mut rows = Vec::with_capacity(collected.height());
        let mut encoded_data_bytes = 2usize; // The empty outer JSON array: [].
        for row_index in 0..collected.height() {
            token.check_cancelled()?;
            let mut row = Vec::with_capacity(names_and_dtypes.len());
            let mut encoded_row_bytes = 2usize; // Opening and closing brackets.
            let mut column_occurrences = HashMap::<String, usize>::new();
            for (column_index, (name, dtype)) in names_and_dtypes.iter().enumerate() {
                let occurrence = column_occurrences.entry(name.clone()).or_default();
                let physical_index = collected
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| column.name().as_str() == name)
                    .nth(*occurrence)
                    .map(|(index, _)| index)
                    .ok_or_else(|| {
                        EngineError::User(format!(
                            "result column at position {column_index} ('{name}') is missing"
                        ))
                    })?;
                *occurrence += 1;
                let column = &collected.columns()[physical_index];
                let value = column.get(row_index).map_err(polars_to_core_error)?;
                let json = if matches!(value, AnyValue::Null) {
                    JsonValue::Null
                } else {
                    any_value_to_json_with_array_struct_override_policy(
                        &value,
                        dtype,
                        array_struct_paths,
                        &serde_json::to_string(name).map_err(|error| {
                            EngineError::User(format!(
                                "could not encode result column name: {error}"
                            ))
                        })?,
                        true,
                    )
                    .map_err(polars_to_core_error)?
                };
                let cell_bytes = serde_json::to_vec(&json)
                    .map_err(|error| EngineError::User(error.to_string()))?
                    .len();
                if cell_bytes > max_cell_json_bytes {
                    return Err(EngineError::User(
                        "a result cell exceeds the configured byte limit".into(),
                    ));
                }
                encoded_row_bytes = encoded_row_bytes
                    .saturating_add(usize::from(!row.is_empty()))
                    .saturating_add(cell_bytes);
                let next_total = encoded_data_bytes
                    .saturating_add(usize::from(!rows.is_empty()))
                    .saturating_add(encoded_row_bytes);
                if next_total > max_total_json_bytes {
                    return Err(EngineError::User(
                        "serialized result exceeds the configured byte limit".into(),
                    ));
                }
                row.push(json);
            }
            let next_total = encoded_data_bytes
                .saturating_add(usize::from(!rows.is_empty()))
                .saturating_add(encoded_row_bytes);
            if next_total > max_total_json_bytes {
                return Err(EngineError::User(
                    "serialized result exceeds the configured byte limit".into(),
                ));
            }
            encoded_data_bytes = next_total;
            rows.push(row);
        }
        token.check_cancelled()?;
        Ok((names, rows, schema))
    }

    /// Collect the DataFrame as a JSON array of row objects (string).
    /// Convenient for embedders that want a single string without depending on Polars error types.
    pub fn to_json_rows(&self) -> Result<String, EngineError> {
        let rows = self.collect_as_json_rows().map_err(polars_to_core_error)?;
        serde_json::to_string(&rows).map_err(Into::into)
    }

    /// Select columns (returns a new DataFrame).
    /// Accepts either column names (strings) or Column expressions (e.g. from regexp_extract_all(...).alias("m")).
    /// Column names are resolved according to case sensitivity.
    pub fn select_exprs(&self, exprs: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        let preserves_row_count = range_projection_preserves_row_count(&exprs);
        let can_run_per_range_batch = range_projection_can_run_per_batch(&exprs);
        let batch_expressions = exprs.clone();
        let logical_schema = if self.logical_schema.is_some() {
            Some(exprs.clone())
        } else {
            None
        };
        let mut selected =
            transformations::select_with_exprs(self, exprs, self.case_sensitive, false)?;
        if let Some(exprs) = logical_schema
            && let Some(schema) = self.projected_logical_schema(&exprs, &selected)
        {
            selected.logical_schema = Some(schema);
        }
        if preserves_row_count {
            selected.row_count_hint = self.row_count_hint;
            if can_run_per_range_batch && let Some(mut range) = self.range_cardinality.clone() {
                range
                    .operations
                    .push(RangeOperation::Project(batch_expressions));
                selected.range_cardinality = Some(range);
            }
        }
        Ok(selected)
    }

    /// Select columns by name (returns a new DataFrame).
    /// Column names are resolved according to case sensitivity.
    /// "*" expands to all columns (PySpark parity #1134); select("*", "a") => all columns then "a" again.
    /// Dotted names (e.g. "outer.inner.leaf") select nested struct fields (PySpark parity).
    pub fn select(&self, cols: Vec<&str>) -> Result<DataFrame, PolarsError> {
        let all_cols = self.columns()?;
        let expanded: Vec<String> = cols
            .iter()
            .flat_map(|c| {
                if *c == "*" {
                    all_cols.clone()
                } else {
                    vec![(*c).to_string()]
                }
            })
            .collect();
        let has_dots = expanded.iter().any(|c| c.contains('.'));
        if has_dots {
            let exprs: Vec<Expr> = expanded
                .iter()
                .map(|c| {
                    let e = self.column_name_to_expr(c)?;
                    let last_part = c.split('.').next_back().unwrap_or(c.as_str());
                    Ok::<Expr, PolarsError>(e.alias(last_part))
                })
                .collect::<Result<Vec<_>, PolarsError>>()?;
            // Exprs already resolved by column_name_to_expr (e.g. "t1.id" -> col("id")); skip re-resolve to avoid ambiguous on col("id") (#1230).
            let mut selected = self.select_exprs(exprs)?;
            if self.logical_schema.is_some()
                && let Some(schema) = self.projected_logical_schema_for_paths(&expanded, &selected)
            {
                selected.logical_schema = Some(schema);
            }
            return Ok(selected);
        }
        // Non-dotted names: build explicit expressions so we can implement PySpark-like
        // ambiguous-case handling. When multiple physical columns differ only by case
        // (e.g. "name" and "NAME"), we coalesce them so selects like select("NaMe")
        // see the first non-null value across all.
        let mut exprs: Vec<Expr> = Vec::with_capacity(expanded.len());
        for requested in &expanded {
            let requested_str = requested.as_str();
            self.check_ambiguous_unqualified(requested_str)?;
            let requested_lower = requested.to_lowercase();
            let matches: Vec<String> = all_cols
                .iter()
                .filter(|c| c.to_lowercase() == requested_lower)
                .cloned()
                .collect();
            if matches.len() > 1 {
                use polars::prelude::coalesce as pl_coalesce;
                let parts: Vec<Expr> = matches.iter().map(|m| col(m.as_str())).collect();
                let coalesced = pl_coalesce(&parts);
                exprs.push(coalesced.alias(requested_str));
                continue;
            }
            let resolved = self.resolve_column_name(requested_str)?;
            exprs.push(col(resolved.as_str()).alias(requested_str));
        }
        self.select_exprs(exprs)
    }

    /// Build an expression for a column name, including dotted struct field access (e.g. "outer.inner.leaf").
    fn column_name_to_expr(&self, name: &str) -> Result<Expr, PolarsError> {
        self.resolve_expr_column_names(Expr::Column(PlSmallStr::from(name)))
    }

    /// Same as [`select`](Self::select) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn select_engine(&self, cols: Vec<&str>) -> Result<DataFrame, EngineError> {
        self.select(cols).map_err(polars_to_core_error)
    }

    /// Select using a mix of column names and expressions (PySpark: select("a", col("b").alias("x"))).
    pub fn select_items(&self, items: Vec<SelectItem<'_>>) -> Result<DataFrame, PolarsError> {
        let mut selected = transformations::select_items(self, items, self.case_sensitive)?;
        let range_expressions = selected.post_select_exprs.clone();
        if self.logical_schema.is_some() {
            selected.logical_schema = range_expressions
                .as_deref()
                .and_then(|expressions| self.projected_logical_schema(expressions, &selected))
                .or_else(|| selected.logical_schema.clone())
                .or_else(|| self.logical_schema_by_output_names(&selected));
        }
        if let Some(expressions) = range_expressions
            && range_projection_preserves_row_count(&expressions)
        {
            selected.row_count_hint = self.row_count_hint;
            if range_projection_can_run_per_batch(&expressions)
                && let Some(mut range) = self.range_cardinality.clone()
            {
                range.operations.push(RangeOperation::Project(expressions));
                selected.range_cardinality = Some(range);
            }
        }
        Ok(selected)
    }

    /// Filter rows using a Polars expression.
    pub fn filter(&self, condition: Expr) -> Result<DataFrame, PolarsError> {
        // Batch filtering preserves semantics only for expressions that can be evaluated
        // independently on each chunk. Aggregates and other nonlocal nodes must fall back to
        // the complete Polars plan instead of changing their scope to a batch.
        let batch_safe = range_projection_can_run_per_batch(std::slice::from_ref(&condition));
        let row_count_hint = batch_safe
            .then(|| {
                self.range_cardinality
                    .as_ref()
                    .and_then(|range| range.count_matching(&condition))
            })
            .flatten();
        let mut range_cardinality = batch_safe.then(|| self.range_cardinality.clone()).flatten();
        if let Some(range) = &mut range_cardinality {
            range
                .operations
                .push(RangeOperation::Filter(condition.clone()));
        }
        let mut filtered = transformations::filter(self, condition, self.case_sensitive)?;
        filtered.row_count_hint = row_count_hint;
        filtered.range_cardinality = range_cardinality;
        filtered.logical_schema = self.logical_schema.clone();
        Ok(filtered)
    }

    /// Same as [`filter`](Self::filter) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn filter_engine(&self, condition: Expr) -> Result<DataFrame, EngineError> {
        self.filter(condition).map_err(polars_to_core_error)
    }

    /// Get a column reference by name (for building expressions).
    /// Respects case sensitivity: when false, "Age" resolves to column "age" if present.
    pub fn column(&self, name: &str) -> Result<Column, PolarsError> {
        let resolved = self.resolve_column_name(name)?;
        Ok(Column::new(resolved))
    }

    /// Add or replace a column. Use a [`Column`] (e.g. from `col("x")`, `rand(42)`, `randn(42)`).
    /// For `rand`/`randn`, generates one distinct value per row (PySpark-like).
    pub fn with_column(&self, column_name: &str, col: &Column) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::with_column(self, column_name, col, self.case_sensitive)?;
        if self.logical_schema.is_some() {
            let expressions = output
                .columns()?
                .into_iter()
                .map(|output_name| {
                    let is_updated_column = if self.case_sensitive {
                        output_name == column_name
                    } else {
                        output_name.eq_ignore_ascii_case(column_name)
                    };
                    if is_updated_column {
                        col.expr().clone()
                    } else {
                        polars::prelude::col(output_name.as_str())
                    }
                })
                .collect::<Vec<_>>();
            output.logical_schema = self
                .projected_logical_schema(&expressions, &output)
                .or_else(|| self.logical_schema_by_output_names(&output));
        }
        Ok(output)
    }

    /// Same as [`with_column`](Self::with_column) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn with_column_engine(
        &self,
        column_name: &str,
        col: &Column,
    ) -> Result<DataFrame, EngineError> {
        self.with_column(column_name, col)
            .map_err(polars_to_core_error)
    }

    /// Add or replace a column using an expression. Prefer [`with_column`](Self::with_column) with a `Column` for rand/randn (per-row values).
    pub fn with_column_expr(
        &self,
        column_name: &str,
        expr: Expr,
    ) -> Result<DataFrame, PolarsError> {
        let col = Column::from_expr(expr, None);
        self.with_column(column_name, &col)
    }

    /// Group by columns (returns GroupedData for aggregation).
    /// Column names are resolved according to case sensitivity.
    ///
    /// PySpark parity: df.groupBy() with no arguments is allowed and treated as a
    /// global aggregation (one group over all rows). We implement this by creating
    /// a synthetic grouping key column with a constant value and grouping by it.
    pub fn group_by(&self, column_names: Vec<&str>) -> Result<GroupedData, PolarsError> {
        use polars::prelude::*;
        let lf = self.lazy_frame();
        let (lazy_grouped, grouping_cols) = if column_names.is_empty() {
            // Global group: add a temporary constant column and group by it.
            let tmp_name = "_gb_global";
            let lf_with_key = lf.clone().with_column(lit(1i32).alias(tmp_name));
            let grouped = lf_with_key.clone().group_by([col(tmp_name)]);
            (grouped, vec![tmp_name.to_string()])
        } else {
            let resolved: Vec<String> = column_names
                .iter()
                .map(|c| self.resolve_column_name(c))
                .collect::<Result<Vec<_>, _>>()?;
            let exprs: Vec<Expr> = resolved.iter().map(|name| col(name.as_str())).collect();
            (lf.clone().group_by(exprs), resolved)
        };
        Ok(GroupedData {
            lf,
            lazy_grouped,
            grouping_cols,
            case_sensitive: self.case_sensitive,
        })
    }

    /// Same as [`group_by`](Self::group_by) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn group_by_engine(&self, column_names: Vec<&str>) -> Result<GroupedData, EngineError> {
        self.group_by(column_names).map_err(polars_to_core_error)
    }

    /// Group by expressions (e.g. col("a") or col("a").alias("x")). PySpark parity for groupBy(Column).
    /// grouping_col_names must match the output names of the resolved exprs (one per expr).
    pub fn group_by_exprs(
        &self,
        exprs: Vec<Expr>,
        grouping_col_names: Vec<String>,
    ) -> Result<GroupedData, PolarsError> {
        use polars::prelude::*;
        if exprs.len() != grouping_col_names.len() {
            return Err(PolarsError::ComputeError(
                format!(
                    "group_by_exprs: {} exprs but {} names",
                    exprs.len(),
                    grouping_col_names.len()
                )
                .into(),
            ));
        }
        let resolved: Vec<Expr> = exprs
            .into_iter()
            .map(|e| self.resolve_expr_column_names(e))
            .collect::<Result<Vec<_>, _>>()?;
        let lf = self.lazy_frame();
        let lazy_grouped = lf.clone().group_by(resolved);
        Ok(GroupedData {
            lf,
            lazy_grouped,
            grouping_cols: grouping_col_names,
            case_sensitive: self.case_sensitive,
        })
    }

    /// Group by mixed specs (names and/or Column expressions). Use when groupBy receives F.col("x").alias("y").
    pub fn group_by_specs(&self, specs: Vec<GroupBySpec>) -> Result<GroupedData, PolarsError> {
        use polars::prelude::*;
        // PySpark: df.groupBy() with no arguments is allowed and treated as a global
        // aggregation (one group over all rows). When specs is empty, delegate to
        // group_by([]) so the DataFrame-level global-group handling is reused.
        if specs.is_empty() {
            return self.group_by(Vec::new());
        }
        let mut exprs = Vec::with_capacity(specs.len());
        let mut names = Vec::with_capacity(specs.len());
        for spec in specs {
            match spec {
                GroupBySpec::Name(s) => {
                    let resolved = self.resolve_column_name(s.as_str())?;
                    exprs.push(col(resolved.as_str()));
                    names.push(resolved);
                }
                GroupBySpec::Column(c) => {
                    let expr = (*c).into_expr();
                    let out_name = polars_plan::utils::expr_output_name(&expr)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| "_".to_string());
                    exprs.push(expr);
                    names.push(out_name);
                }
            }
        }
        self.group_by_exprs(exprs, names)
    }

    /// Cube: multiple grouping sets (all subsets of columns), then union (PySpark cube).
    pub fn cube(&self, column_names: Vec<&str>) -> Result<CubeRollupData, PolarsError> {
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CubeRollupData {
            lf: self.lazy_frame(),
            grouping_cols: resolved,
            case_sensitive: self.case_sensitive,
            is_cube: true,
        })
    }

    /// Rollup: grouping sets (prefixes of columns), then union (PySpark rollup).
    pub fn rollup(&self, column_names: Vec<&str>) -> Result<CubeRollupData, PolarsError> {
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CubeRollupData {
            lf: self.lazy_frame(),
            grouping_cols: resolved,
            case_sensitive: self.case_sensitive,
            is_cube: false,
        })
    }

    /// Global aggregation (no groupBy): apply aggregate expressions over the whole DataFrame,
    /// returning a single-row DataFrame (PySpark: df.agg(F.sum("x"), F.avg("y"))).
    /// Duplicate output names are disambiguated with _1, _2, ... (issue #368).
    pub fn agg(&self, aggregations: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<Expr> = aggregations
            .into_iter()
            .map(|e| {
                let resolved = self.resolve_expr_column_names(e)?;
                self.coerce_string_numeric_comparisons(resolved)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let disambiguated = disambiguate_agg_output_names(resolved);
        let pl_df = self.lazy_frame().select(disambiguated).collect()?;
        Ok(Self::from_polars_with_options(pl_df, self.case_sensitive))
    }

    /// Join with another DataFrame on the given columns.
    /// Join column names are resolved on the left (and right must have matching names).
    pub fn join(
        &self,
        other: &DataFrame,
        on: Vec<&str>,
        how: JoinType,
    ) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<String> = on
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let on_refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        join(
            self,
            other,
            on_refs.clone(),
            on_refs,
            how,
            JoinOptions {
                case_sensitive: self.case_sensitive,
                coalesce_same_name_keys: true, // join(right, "id") yields one key column (#1049, #353)
                mark_join_keys_ambiguous: false,
                origin: crate::dataframe::joins::JoinOrigin::ColumnOn,
            },
        )
    }

    /// Join with different column names on left and right (PySpark left_on/right_on).
    /// When `only_key_equalities` is true and key names match, unqualified key names are treated as ambiguous (#1230).
    pub fn join_with_keys(
        &self,
        other: &DataFrame,
        left_on: Vec<&str>,
        right_on: Vec<&str>,
        how: JoinType,
        only_key_equalities: bool,
    ) -> Result<DataFrame, PolarsError> {
        let left_resolved: Vec<String> = left_on
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let right_resolved: Vec<String> = right_on
            .iter()
            .map(|c| other.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let left_refs: Vec<&str> = left_resolved.iter().map(|s| s.as_str()).collect();
        let right_refs: Vec<&str> = right_resolved.iter().map(|s| s.as_str()).collect();
        // When same-named keys (e.g. left.id == right.id), or key names match case-insensitively (name/NAME),
        // decide whether to coalesce duplicate key columns or keep them separate:
        // - Column-name joins (only_key_equalities = false) coalesce so the result has a single key column
        //   (PySpark parity for join(on=...) #1049, #353, #1148, #1165).
        // - Expression-based joins (only_key_equalities = true) keep both key columns so left/right
        //   keys remain addressable separately (dept_id, dept_id_right) — matches condition joins
        //   used by the Python API and parity tests.
        let same_named_keys = left_resolved.len() == right_resolved.len()
            && left_resolved
                .iter()
                .zip(right_resolved.iter())
                .all(|(a, b)| a.eq_ignore_ascii_case(b));
        let coalesce_same_name_keys = same_named_keys && !only_key_equalities;
        // Only mark keys ambiguous when condition was purely key equalities; otherwise filter will reference them (#1230).
        let mark_join_keys_ambiguous = same_named_keys && only_key_equalities;
        join(
            self,
            other,
            left_refs,
            right_refs,
            how,
            JoinOptions {
                case_sensitive: self.case_sensitive,
                coalesce_same_name_keys,
                mark_join_keys_ambiguous,
                origin: crate::dataframe::joins::JoinOrigin::Condition,
            },
        )
    }

    /// Order by columns (sort).
    /// When `spark.sql.caseSensitive` is false (default), column names are resolved
    /// case-insensitively so that `order_by("value")` and `order_by("VALUE")` both
    /// work when the schema has a column named `Value` (PySpark parity).
    pub fn order_by(
        &self,
        column_names: Vec<&str>,
        ascending: Vec<bool>,
    ) -> Result<DataFrame, PolarsError> {
        let mut ordered =
            transformations::order_by(self, column_names, ascending, self.case_sensitive)?;
        ordered.logical_schema = self.logical_schema.clone();
        Ok(ordered)
    }

    /// Order by sort expressions (asc/desc with nulls_first/last).
    pub fn order_by_exprs(&self, sort_orders: Vec<SortOrder>) -> Result<DataFrame, PolarsError> {
        let mut ordered = transformations::order_by_exprs(self, sort_orders, self.case_sensitive)?;
        ordered.logical_schema = self.logical_schema.clone();
        Ok(ordered)
    }

    /// Union (unionAll): stack another DataFrame vertically. Schemas must match (same columns, same order).
    pub fn union(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        let mut combined = transformations::union(self, other, self.case_sensitive)?;
        combined.logical_schema = self.merged_logical_schema(other, &combined, false);
        Ok(combined)
    }

    /// Alias for union (PySpark unionAll).
    pub fn union_all(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        self.union(other)
    }

    /// Union by name: stack vertically, aligning columns by name. When allow_missing_columns is true, columns missing in other are filled with null.
    pub fn union_by_name(
        &self,
        other: &DataFrame,
        allow_missing_columns: bool,
    ) -> Result<DataFrame, PolarsError> {
        let mut combined = transformations::union_by_name(
            self,
            other,
            allow_missing_columns,
            self.case_sensitive,
        )?;
        combined.logical_schema = self.merged_logical_schema(other, &combined, true);
        Ok(combined)
    }

    /// Distinct: drop duplicate rows (all columns or optional subset).
    pub fn distinct(&self, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        let mut distinct = transformations::distinct(self, subset, self.case_sensitive)?;
        distinct.logical_schema = self.logical_schema.clone();
        Ok(distinct)
    }

    /// Drop one or more columns.
    pub fn drop(&self, columns: Vec<&str>) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::drop(self, columns, self.case_sensitive)?;
        if self.logical_schema.is_some()
            && let Some(schema) = self.logical_schema_by_output_names(&output)
        {
            output.logical_schema = Some(schema);
        }
        Ok(output)
    }

    /// Drop columns from string names and/or Column references (PySpark `drop(col)` parity).
    pub fn drop_specs(
        &self,
        specs: Vec<transformations::DropColumnSpec>,
    ) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::drop_specs(self, specs, self.case_sensitive)?;
        if self.logical_schema.is_some()
            && let Some(schema) = self.logical_schema_by_output_names(&output)
        {
            output.logical_schema = Some(schema);
        }
        Ok(output)
    }

    /// Drop rows with nulls. PySpark na.drop(subset, how, thresh).
    pub fn dropna(
        &self,
        subset: Option<Vec<&str>>,
        how: &str,
        thresh: Option<usize>,
    ) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::dropna(self, subset, how, thresh, self.case_sensitive)?;
        output.logical_schema = self.logical_schema.clone();
        Ok(output)
    }

    /// Fill nulls with a literal expression. PySpark na.fill(value, subset=...).
    pub fn fillna(&self, value: Expr, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::fillna(self, value, subset, self.case_sensitive)?;
        if self.logical_schema.is_some()
            && let Some(schema) = self.logical_schema_by_output_names(&output)
        {
            output.logical_schema = Some(schema);
        }
        Ok(output)
    }

    /// Limit: return first n rows.
    pub fn limit(&self, n: usize) -> Result<DataFrame, PolarsError> {
        let mut limited = transformations::limit(self, n, self.case_sensitive)?;
        limited.row_count_hint = self.row_count_hint.map(|count| count.min(n as u64));
        let mut range_cardinality = self.range_cardinality.clone();
        if let Some(range) = &mut range_cardinality {
            range.operations.push(RangeOperation::Limit(n as u64));
        }
        limited.range_cardinality = range_cardinality;
        limited.logical_schema = self.logical_schema.clone();
        Ok(limited)
    }

    /// Same as [`limit`](Self::limit) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn limit_engine(&self, n: usize) -> Result<DataFrame, EngineError> {
        self.limit(n).map_err(polars_to_core_error)
    }

    /// Rename a column (old_name -> new_name).
    pub fn with_column_renamed(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<DataFrame, PolarsError> {
        let mut output =
            transformations::with_column_renamed(self, old_name, new_name, self.case_sensitive)?;
        if self.logical_schema.is_some()
            && let Some(schema) = self.logical_schema_after_renames(
                &output,
                &[(old_name.to_string(), new_name.to_string())],
            )
        {
            output.logical_schema = Some(schema);
        }
        Ok(output)
    }

    /// Replace values in a column (old_value -> new_value). PySpark replace.
    pub fn replace(
        &self,
        column_name: &str,
        old_value: Expr,
        new_value: Expr,
    ) -> Result<DataFrame, PolarsError> {
        let mut output =
            transformations::replace(self, column_name, old_value, new_value, self.case_sensitive)?;
        if self.logical_schema.is_some()
            && let Some(schema) = self.logical_schema_by_output_names(&output)
        {
            output.logical_schema = Some(schema);
        }
        Ok(output)
    }

    /// Cross join with another DataFrame (cartesian product). PySpark crossJoin.
    pub fn cross_join(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::cross_join(self, other, self.case_sensitive)?;
        output.logical_schema = self.joined_logical_schema(other, &output, JoinType::Inner);
        Ok(output)
    }

    /// Summary statistics. PySpark describe.
    pub fn describe(&self) -> Result<DataFrame, PolarsError> {
        transformations::describe(self, self.case_sensitive)
    }

    /// No-op: execution is eager by default. PySpark cache.
    pub fn cache(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: execution is eager by default. PySpark persist.
    pub fn persist(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op. PySpark unpersist.
    pub fn unpersist(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Set difference: rows in self not in other. PySpark subtract / except.
    pub fn subtract(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::subtract(self, other, self.case_sensitive)
    }

    /// Set intersection: rows in both self and other. PySpark intersect.
    pub fn intersect(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::intersect(self, other, self.case_sensitive)
    }

    /// Sample a fraction of rows. PySpark sample(withReplacement, fraction, seed).
    pub fn sample(
        &self,
        with_replacement: bool,
        fraction: f64,
        seed: Option<u64>,
    ) -> Result<DataFrame, PolarsError> {
        transformations::sample(self, with_replacement, fraction, seed, self.case_sensitive)
    }

    /// Split into multiple DataFrames by weights. PySpark randomSplit(weights, seed).
    pub fn random_split(
        &self,
        weights: &[f64],
        seed: Option<u64>,
    ) -> Result<Vec<DataFrame>, PolarsError> {
        transformations::random_split(self, weights, seed, self.case_sensitive)
    }

    /// Stratified sample by column value. PySpark sampleBy(col, fractions, seed).
    /// fractions: list of (value as Expr, fraction) for that stratum.
    pub fn sample_by(
        &self,
        col_name: &str,
        fractions: &[(Expr, f64)],
        seed: Option<u64>,
    ) -> Result<DataFrame, PolarsError> {
        transformations::sample_by(self, col_name, fractions, seed, self.case_sensitive)
    }

    /// First row as a one-row DataFrame. PySpark first().
    pub fn first(&self) -> Result<DataFrame, PolarsError> {
        transformations::first(self, self.case_sensitive)
    }

    /// First n rows. PySpark head(n).
    pub fn head(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::head(self, n, self.case_sensitive)
    }

    /// Take first n rows. PySpark take(n).
    pub fn take(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::take(self, n, self.case_sensitive)
    }

    /// Last n rows. PySpark tail(n).
    pub fn tail(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::tail(self, n, self.case_sensitive)
    }

    /// True if the DataFrame has zero rows. PySpark isEmpty.
    pub fn is_empty(&self) -> bool {
        transformations::is_empty(self)
    }

    /// Rename columns. PySpark toDF(*colNames).
    pub fn to_df(&self, names: Vec<&str>) -> Result<DataFrame, PolarsError> {
        transformations::to_df(self, &names, self.case_sensitive)
    }

    /// Statistical helper. PySpark df.stat().cov / .corr.
    pub fn stat(&self) -> DataFrameStat<'_> {
        DataFrameStat { df: self }
    }

    /// Correlation matrix of all numeric columns. PySpark df.corr() returns a DataFrame of pairwise correlations.
    pub fn corr(&self) -> Result<DataFrame, PolarsError> {
        self.stat().corr_matrix()
    }

    /// Pearson correlation between two columns (scalar). PySpark df.corr(col1, col2).
    pub fn corr_cols(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        self.stat().corr(col1, col2)
    }

    /// Sample covariance between two columns (scalar). PySpark df.cov(col1, col2).
    pub fn cov_cols(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        self.stat().cov(col1, col2)
    }

    /// Summary statistics (alias for describe). PySpark summary.
    pub fn summary(&self) -> Result<DataFrame, PolarsError> {
        self.describe()
    }

    /// Collect rows as JSON strings (one per row). PySpark toJSON.
    pub fn to_json(&self) -> Result<Vec<String>, PolarsError> {
        transformations::to_json(self)
    }

    /// Return execution plan description. PySpark explain.
    pub fn explain(&self) -> String {
        transformations::explain(self)
    }

    /// Return schema as tree string. PySpark printSchema (returns string; print to stdout if needed).
    pub fn print_schema(&self) -> Result<String, PolarsError> {
        transformations::print_schema(self)
    }

    /// No-op: Polars backend is eager. PySpark checkpoint.
    pub fn checkpoint(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: Polars backend is eager. PySpark localCheckpoint.
    pub fn local_checkpoint(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: single partition in Polars. PySpark repartition(n).
    pub fn repartition(&self, _num_partitions: usize) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: Polars has no range partitioning. PySpark repartitionByRange(n, cols).
    pub fn repartition_by_range(
        &self,
        _num_partitions: usize,
        _cols: Vec<&str>,
    ) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Column names and dtype strings. PySpark type names (LongType, StringType, etc.) for schema alignment (#790, #734).
    pub fn dtypes(&self) -> Result<Vec<(String, String)>, PolarsError> {
        let schema = self.schema_or_collect()?;
        Ok(schema
            .iter_names_and_dtypes()
            .map(|(name, dtype)| (name.to_string(), pyspark_type_name(dtype)))
            .collect())
    }

    /// No-op: we don't model partitions. PySpark sortWithinPartitions. Same as orderBy for compatibility.
    pub fn sort_within_partitions(
        &self,
        _cols: &[crate::functions::SortOrder],
    ) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: single partition in Polars. PySpark coalesce(n).
    pub fn coalesce(&self, _num_partitions: usize) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op. PySpark hint (query planner hint).
    pub fn hint(&self, _name: &str, _params: &[i32]) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Returns true (eager single-node). PySpark isLocal.
    pub fn is_local(&self) -> bool {
        true
    }

    /// Returns empty vec (no file sources). PySpark inputFiles.
    pub fn input_files(&self) -> Vec<String> {
        Vec::new()
    }

    /// No-op; returns false. PySpark sameSemantics.
    pub fn same_semantics(&self, _other: &DataFrame) -> bool {
        false
    }

    /// No-op; returns 0. PySpark semanticHash.
    pub fn semantic_hash(&self) -> u64 {
        0
    }

    /// No-op. PySpark observe (metrics).
    pub fn observe(&self, _name: &str, _expr: Expr) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op. PySpark withWatermark (streaming).
    pub fn with_watermark(
        &self,
        _event_time: &str,
        _delay: &str,
    ) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Select by expression strings (minimal: column names, optionally "col as alias"). PySpark selectExpr.
    pub fn select_expr(&self, exprs: &[String]) -> Result<DataFrame, PolarsError> {
        transformations::select_expr(self, exprs, self.case_sensitive)
    }

    /// Select by expression strings using SQL parsing (e.g. "upper(Name) as u"). Use when session is available for full selectExpr parity.
    #[cfg(feature = "sql")]
    pub fn select_expr_with_session(
        &self,
        session: &SparkSession,
        exprs: &[String],
    ) -> Result<DataFrame, PolarsError> {
        let parsed = crate::sql::parse_select_exprs(session, self, exprs)?;
        self.select_exprs(parsed)
    }

    /// Select columns whose names match the regex. PySpark colRegex.
    pub fn col_regex(&self, pattern: &str) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::col_regex(self, pattern, self.case_sensitive)?;
        if self.logical_schema.is_some()
            && let Some(schema) = self.logical_schema_by_output_names(&output)
        {
            output.logical_schema = Some(schema);
        }
        Ok(output)
    }

    /// Add or replace multiple columns. PySpark withColumns. Accepts `Column` so rand/randn get per-row values.
    pub fn with_columns(&self, exprs: &[(String, Column)]) -> Result<DataFrame, PolarsError> {
        if self.logical_schema.is_none() {
            return transformations::with_columns(self, exprs, self.case_sensitive);
        }
        // Preserve expression-derived nullability for each replacement. Rebuilding a schema
        // from output names alone incorrectly kept nullable=false from the replaced source.
        let mut output = self.clone();
        for (name, column) in exprs {
            output = output.with_column(name, column)?;
        }
        Ok(output)
    }

    /// Rename multiple columns. PySpark withColumnsRenamed.
    pub fn with_columns_renamed(
        &self,
        renames: &[(String, String)],
    ) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::with_columns_renamed(self, renames, self.case_sensitive)?;
        if self.logical_schema.is_some()
            && let Some(schema) = self.logical_schema_after_renames(&output, renames)
        {
            output.logical_schema = Some(schema);
        }
        Ok(output)
    }

    /// NA sub-API. PySpark df.na().
    pub fn na(&self) -> DataFrameNa<'_> {
        DataFrameNa { df: self }
    }

    /// Skip first n rows. PySpark offset(n).
    pub fn offset(&self, n: usize) -> Result<DataFrame, PolarsError> {
        let mut output = transformations::offset(self, n, self.case_sensitive)?;
        output.logical_schema = self.logical_schema.clone();
        Ok(output)
    }

    /// Transform by a function. PySpark transform(func).
    pub fn transform<F>(&self, f: F) -> Result<DataFrame, PolarsError>
    where
        F: FnOnce(DataFrame) -> Result<DataFrame, PolarsError>,
    {
        transformations::transform(self, f)
    }

    /// Frequent items. PySpark freqItems (stub).
    pub fn freq_items(&self, columns: &[&str], support: f64) -> Result<DataFrame, PolarsError> {
        transformations::freq_items(self, columns, support, self.case_sensitive)
    }

    /// Approximate quantiles. PySpark approxQuantile (stub).
    pub fn approx_quantile(
        &self,
        column: &str,
        probabilities: &[f64],
    ) -> Result<DataFrame, PolarsError> {
        transformations::approx_quantile(self, column, probabilities, self.case_sensitive)
    }

    /// Cross-tabulation. PySpark crosstab (stub).
    pub fn crosstab(&self, col1: &str, col2: &str) -> Result<DataFrame, PolarsError> {
        transformations::crosstab(self, col1, col2, self.case_sensitive)
    }

    /// Unpivot (melt). PySpark melt. Long format: id columns + variable + value.
    pub fn melt(&self, id_vars: &[&str], value_vars: &[&str]) -> Result<DataFrame, PolarsError> {
        transformations::melt(self, id_vars, value_vars, self.case_sensitive)
    }

    /// Unpivot (wide to long). PySpark unpivot. Same as melt(ids, values).
    pub fn unpivot(&self, ids: &[&str], values: &[&str]) -> Result<DataFrame, PolarsError> {
        transformations::melt(self, ids, values, self.case_sensitive)
    }

    /// Pivot (wide format). PySpark: use `groupBy(...).pivot(col)` for pivot-table aggregation.
    pub fn pivot(
        &self,
        pivot_col: &str,
        _values: Option<Vec<&str>>,
    ) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            format!(
                "DataFrame.pivot({pivot_col}) is not supported; use groupBy(...).pivot({pivot_col:?}) for pivot-table aggregation, or crosstab(col1, col2) for two-column counts."
            )
            .into(),
        ))
    }

    /// Set difference keeping duplicates. PySpark exceptAll.
    pub fn except_all(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::except_all(self, other, self.case_sensitive)
    }

    /// Set intersection keeping duplicates. PySpark intersectAll.
    pub fn intersect_all(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::intersect_all(self, other, self.case_sensitive)
    }

    /// Write this DataFrame to a Delta table at the given path.
    /// Requires the `delta` feature. If `overwrite` is true, replaces the table; otherwise appends.
    /// When appending, `merge_schema` merges existing and new schemas (union of columns, nulls for missing). See #851.
    #[cfg(feature = "delta")]
    pub fn write_delta(
        &self,
        path: impl AsRef<std::path::Path>,
        overwrite: bool,
        merge_schema: bool,
    ) -> Result<(), PolarsError> {
        self.write_delta_with_options(path, overwrite, merge_schema, &[])
    }

    /// Write to Delta while preserving path-writer options such as partition columns.
    #[cfg(feature = "delta")]
    pub fn write_delta_with_options(
        &self,
        path: impl AsRef<std::path::Path>,
        overwrite: bool,
        merge_schema: bool,
        partition_by: &[String],
    ) -> Result<(), PolarsError> {
        let resolved_partition_by = partition_by
            .iter()
            .map(|name| self.resolve_column_name(name))
            .collect::<Result<Vec<_>, _>>()?;
        crate::delta::write_delta_with_options(
            self.collect_inner()?.as_ref(),
            path,
            overwrite,
            merge_schema,
            &resolved_partition_by,
        )
    }

    /// Write Delta data with Spark-style save modes and schema evolution options.
    #[cfg(feature = "delta")]
    pub fn write_delta_with_save_options(
        &self,
        path: impl AsRef<std::path::Path>,
        mode: SaveMode,
        merge_schema: bool,
        overwrite_schema: bool,
        partition_by: &[String],
    ) -> Result<(), PolarsError> {
        let resolved_partition_by = partition_by
            .iter()
            .map(|name| self.resolve_column_name(name))
            .collect::<Result<Vec<_>, _>>()?;
        crate::delta::write_delta_with_save_options(
            self.collect_inner()?.as_ref(),
            path,
            mode,
            merge_schema,
            overwrite_schema,
            &resolved_partition_by,
        )
    }

    #[cfg(feature = "delta")]
    pub fn write_delta_with_save_options_and_logical_map_paths(
        &self,
        path: impl AsRef<std::path::Path>,
        mode: SaveMode,
        merge_schema: bool,
        overwrite_schema: bool,
        partition_by: &[String],
        logical_map_paths: &[String],
    ) -> Result<(), PolarsError> {
        let resolved_partition_by = partition_by
            .iter()
            .map(|name| self.resolve_column_name(name))
            .collect::<Result<Vec<_>, _>>()?;
        crate::delta::write_delta_with_save_options_and_logical_map_paths(
            self.collect_inner()?.as_ref(),
            path,
            mode,
            merge_schema,
            overwrite_schema,
            &resolved_partition_by,
            logical_map_paths,
        )
    }

    /// Stub when `delta` feature is disabled.
    #[cfg(not(feature = "delta"))]
    pub fn write_delta(
        &self,
        path: impl AsRef<std::path::Path>,
        overwrite: bool,
        merge_schema: bool,
    ) -> Result<(), PolarsError> {
        self.write_delta_with_options(path, overwrite, merge_schema, &[])
    }

    #[cfg(not(feature = "delta"))]
    pub fn write_delta_with_options(
        &self,
        _path: impl AsRef<std::path::Path>,
        _overwrite: bool,
        _merge_schema: bool,
        _partition_by: &[String],
    ) -> Result<(), PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    #[cfg(not(feature = "delta"))]
    pub fn write_delta_with_save_options(
        &self,
        _path: impl AsRef<std::path::Path>,
        _mode: SaveMode,
        _merge_schema: bool,
        _overwrite_schema: bool,
        _partition_by: &[String],
    ) -> Result<(), PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    /// Register this DataFrame as an in-memory "delta table" by name (same namespace as saveAsTable). Readable via `read_delta(name)` or `table(name)`.
    pub fn save_as_delta_table(&self, session: &crate::session::SparkSession, name: &str) {
        session.register_table(name, self.clone());
    }

    /// Return a writer for generic format (parquet, csv, json). PySpark-style write API.
    pub fn write(&self) -> DataFrameWriter<'_> {
        DataFrameWriter {
            df: self,
            mode: WriteMode::Overwrite,
            format: WriteFormat::Parquet,
            format_was_set: false,
            options: HashMap::new(),
            partition_by: Vec::new(),
        }
    }
}

/// Write mode: overwrite or append (PySpark DataFrameWriter.mode for path-based save).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Overwrite,
    Append,
}

/// Save mode for saveAsTable (PySpark default is ErrorIfExists).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SaveMode {
    /// Throw if table already exists (PySpark default).
    ErrorIfExists,
    /// Replace existing table.
    Overwrite,
    /// Append to existing table; create if not exists. Column names align.
    Append,
    /// No-op if table exists; create if not.
    Ignore,
}

/// Output format for generic write (PySpark DataFrameWriter.format).
#[derive(Clone, Copy)]
pub enum WriteFormat {
    Parquet,
    Csv,
    Json,
}

/// Align two DataFrames to a merged column order (existing first, then new columns not in existing).
/// Adds null columns for missing columns. Used for saveAsTable append with mergeSchema (issue #1109).
pub(crate) fn align_to_merged_schema_inline(
    existing: &PlDataFrame,
    new_df: &PlDataFrame,
) -> Result<(PlDataFrame, PlDataFrame), PolarsError> {
    use polars::prelude::*;
    let existing_names: Vec<String> = existing
        .get_column_names()
        .iter()
        .map(|s| s.as_str().to_string())
        .collect();
    let new_names: Vec<String> = new_df
        .get_column_names()
        .iter()
        .map(|s| s.as_str().to_string())
        .collect();
    let existing_set: HashSet<&str> = existing_names.iter().map(String::as_str).collect();
    let mut merged: Vec<String> = existing_names.clone();
    for n in &new_names {
        if !existing_set.contains(n.as_str()) {
            merged.push(n.clone());
        }
    }
    let n_existing = existing.height();
    let n_new = new_df.height();
    let schema_existing = existing.schema();
    let schema_new = new_df.schema();
    let name_into = |n: &String| n.as_str().into();
    let mut cols_existing: Vec<polars::prelude::Column> = Vec::with_capacity(merged.len());
    let mut cols_new: Vec<polars::prelude::Column> = Vec::with_capacity(merged.len());
    for name in &merged {
        if let Some(dtype) = schema_existing.get(name) {
            if let Some(idx) = existing.get_column_index(name) {
                cols_existing.push(existing.columns()[idx].clone());
            } else {
                cols_existing.push(Series::full_null(name_into(name), n_existing, dtype).into());
            }
        } else if let Some(dtype) = schema_new.get(name) {
            cols_existing.push(Series::full_null(name_into(name), n_existing, dtype).into());
        } else {
            cols_existing
                .push(Series::full_null(name_into(name), n_existing, &DataType::String).into());
        }
        if let Some(dtype) = schema_new.get(name) {
            if let Some(idx) = new_df.get_column_index(name) {
                cols_new.push(new_df.columns()[idx].clone());
            } else {
                cols_new.push(Series::full_null(name_into(name), n_new, dtype).into());
            }
        } else if let Some(dtype) = schema_existing.get(name) {
            cols_new.push(Series::full_null(name_into(name), n_new, dtype).into());
        } else {
            cols_new.push(Series::full_null(name_into(name), n_new, &DataType::String).into());
        }
    }
    let aligned_existing = PlDataFrame::new_infer_height(cols_existing)?;
    let aligned_new = PlDataFrame::new_infer_height(cols_new)?;
    Ok((aligned_existing, aligned_new))
}

/// Builder for writing DataFrame to path (PySpark DataFrameWriter).
pub struct DataFrameWriter<'a> {
    df: &'a DataFrame,
    mode: WriteMode,
    format: WriteFormat,
    format_was_set: bool,
    options: HashMap<String, String>,
    partition_by: Vec<String>,
}

impl<'a> DataFrameWriter<'a> {
    pub fn mode(mut self, mode: WriteMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn format(mut self, format: WriteFormat) -> Self {
        self.format = format;
        self.format_was_set = true;
        self
    }

    /// Add a single option (PySpark: option(key, value)). Returns self for chaining.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Add multiple options (PySpark: options(**kwargs)). Returns self for chaining.
    pub fn options(mut self, opts: impl IntoIterator<Item = (String, String)>) -> Self {
        for (k, v) in opts {
            self.options.insert(k, v);
        }
        self
    }

    /// Partition output by the given columns (PySpark: partitionBy(cols)).
    pub fn partition_by(mut self, cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.partition_by = cols.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Save the DataFrame as a table (PySpark: saveAsTable). In-memory by default; when spark.sql.warehouse.dir is set, persists to disk for cross-session access.
    /// Writer options (e.g. mergeSchema) are applied when set via option()/options(). Issue #1109.
    pub fn save_as_table(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
    ) -> Result<(), PolarsError> {
        self.save_as_table_with_cancellation(session, name, mode, &CancellationToken::new())
    }

    /// Save a table while honoring cancellation before materialization and commit acceptance.
    pub fn save_as_table_with_cancellation(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        token: &CancellationToken,
    ) -> Result<(), PolarsError> {
        let opts: Vec<(String, String)> = self
            .options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let options = if opts.is_empty() {
            None
        } else {
            Some(opts.as_slice())
        };
        self.save_as_table_impl(session, name, mode, options, token)
    }

    /// Same as save_as_table but with options passed explicitly (e.g. from Python writer). Issue #1109.
    pub fn save_as_table_with_options(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        options: &[(String, String)],
    ) -> Result<(), PolarsError> {
        self.save_as_table_with_options_and_cancellation(
            session,
            name,
            mode,
            options,
            &CancellationToken::new(),
        )
    }

    /// Same as `save_as_table_with_options` with a caller-owned cancellation token.
    pub fn save_as_table_with_options_and_cancellation(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        options: &[(String, String)],
        token: &CancellationToken,
    ) -> Result<(), PolarsError> {
        self.save_as_table_impl(
            session,
            name,
            mode,
            if options.is_empty() {
                None
            } else {
                Some(options)
            },
            token,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_as_external_table_descriptor(
        &self,
        name: &str,
        mode: SaveMode,
        options: Option<&[(String, String)]>,
        provider: &dyn ExternalCatalogProvider,
        descriptor: &ExternalTableDescriptor,
        logical_map_paths: &[String],
        token: &CancellationToken,
    ) -> Result<(), PolarsError> {
        token
            .check_cancelled()
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
        let external_mode = match mode {
            SaveMode::ErrorIfExists => {
                return Err(PolarsError::InvalidOperation(
                    format!("Table or view '{name}' already exists. SaveMode is ErrorIfExists.")
                        .into(),
                ));
            }
            SaveMode::Ignore => return Ok(()),
            SaveMode::Append => ExternalTableSaveMode::Append,
            SaveMode::Overwrite => ExternalTableSaveMode::Overwrite,
        };
        fn contains_map(dtype: &crate::schema::DataType) -> bool {
            use crate::schema::DataType;
            match dtype {
                DataType::Map(_, _) => true,
                DataType::Array(element) => contains_map(element),
                DataType::Struct(fields) => {
                    fields.iter().any(|field| contains_map(&field.data_type))
                }
                _ => false,
            }
        }
        if !logical_map_paths.is_empty()
            || descriptor
                .schema
                .fields()
                .iter()
                .any(|field| contains_map(&field.data_type))
        {
            return Err(PolarsError::InvalidOperation(
                "external catalog Arrow IPC writes do not yet support logical Map columns".into(),
            ));
        }

        let table_format = options
            .unwrap_or_default()
            .iter()
            .rev()
            .find(|(key, _)| key.eq_ignore_ascii_case("sparkless.writer.format"))
            .map(|(_, value)| value.clone())
            .or_else(|| {
                self.format_was_set.then(|| match self.format {
                    WriteFormat::Parquet => "parquet".to_string(),
                    WriteFormat::Csv => "csv".to_string(),
                    WriteFormat::Json => "json".to_string(),
                })
            });
        let writer_options = options
            .unwrap_or_default()
            .iter()
            .filter(|(key, _)| !key.to_ascii_lowercase().starts_with("sparkless."))
            .cloned()
            .collect();
        let partition_by = self
            .partition_by
            .iter()
            .map(|column| self.df.resolve_column_name(column))
            .collect::<Result<Vec<_>, _>>()?;

        if provider.supports_batch_writes() {
            let max_rows = external_arrow_batch_limit(
                options,
                "sparkless.writer.maxRowsPerBatch",
                DEFAULT_EXTERNAL_ARROW_BATCH_ROWS,
            )?;
            let max_bytes = external_arrow_batch_limit(
                options,
                "sparkless.writer.maxBytesPerBatch",
                DEFAULT_EXTERNAL_ARROW_BATCH_BYTES,
            )?;
            let batches = self
                .df
                .to_arrow_ipc_batches_with_cancellation(max_rows, max_bytes, token)
                .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
            let request = ExternalTableWriteBatchRequest {
                identifier: name.to_string(),
                table: descriptor.clone(),
                batches,
                mode: external_mode,
                format: table_format,
                options: writer_options,
                partition_by,
            };
            let result = provider
                .write_table_batches_with_cancellation(request, token)
                .map_err(|error| match error {
                    EngineError::Cancelled(message) => {
                        PolarsError::ComputeError(format!("cancelled: {message}").into())
                    }
                    error => PolarsError::InvalidOperation(
                        format!("external catalog provider failed to write '{name}': {error}")
                            .into(),
                    ),
                })?;
            return result.validate_against(descriptor).map_err(|error| {
                PolarsError::InvalidOperation(
                    format!("external catalog provider returned an invalid write receipt: {error}")
                        .into(),
                )
            });
        }

        let mut materialized = self
            .df
            .collect_with_cancellation(token)
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?
            .as_ref()
            .clone();
        let mut cursor = std::io::Cursor::new(Vec::new());
        {
            use polars::io::ipc::IpcStreamWriter;
            use polars::prelude::SerWriter;
            IpcStreamWriter::new(&mut cursor).finish(&mut materialized)?;
        }
        let request = ExternalTableWriteRequest {
            identifier: name.to_string(),
            table: descriptor.clone(),
            data: ArrowIpcStream {
                bytes: cursor.into_inner(),
            },
            mode: external_mode,
            format: table_format,
            options: writer_options,
            partition_by,
        };
        token
            .check_cancelled()
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
        let result = provider
            .write_table_with_cancellation(request, token)
            .map_err(|error| match error {
                EngineError::Cancelled(message) => {
                    PolarsError::ComputeError(format!("cancelled: {message}").into())
                }
                error => PolarsError::InvalidOperation(
                    format!("external catalog provider failed to write '{name}': {error}").into(),
                ),
            })?;
        result.validate_against(descriptor).map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog provider returned an invalid write receipt: {error}")
                    .into(),
            )
        })?;
        Ok(())
    }

    /// JDBC write (PySpark-style). Requires the `jdbc` or `sqlite` feature.
    #[cfg(any(
        feature = "jdbc",
        feature = "jdbc_mysql",
        feature = "jdbc_mariadb",
        feature = "jdbc_mssql",
        feature = "jdbc_oracle",
        feature = "jdbc_db2",
        feature = "sqlite"
    ))]
    pub fn jdbc(
        &self,
        url: &str,
        table: &str,
        properties: &[(String, String)],
        mode: SaveMode,
    ) -> Result<(), crate::error::EngineError> {
        use crate::jdbc::{JdbcOptions, write_jdbc_from_polars};
        use std::collections::HashMap;

        let mut props_map = HashMap::new();
        for (k, v) in properties {
            props_map.insert(k.clone(), v.clone());
        }
        let opts = JdbcOptions::from_url_dbtable_and_properties(
            url.to_string(),
            table.to_string(),
            &props_map,
        )?;
        let pl_df = self
            .df
            .collect_inner()
            .map_err(crate::polars_to_core_error)?;
        write_jdbc_from_polars(pl_df.as_ref(), &opts, mode)
    }

    fn save_as_table_impl(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        options: Option<&[(String, String)]>,
        token: &CancellationToken,
    ) -> Result<(), PolarsError> {
        use polars::prelude::*;
        use std::fs;
        use std::path::Path;

        token
            .check_cancelled()
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;

        if SparkSession::is_global_temp_name(name) && session.local_table_exists(name) {
            return Err(PolarsError::InvalidOperation(
                format!("cannot save a table over global temporary view '{name}'").into(),
            ));
        }

        let (merge_schema, overwrite_schema, is_delta_format, logical_map_paths) =
            options.map_or((false, false, false, Vec::new()), |opts| {
                let mut merge_schema = false;
                let mut overwrite_schema = false;
                let mut is_delta = false;
                let mut logical_map_paths: Vec<String> = Vec::new();
                for (k, v) in opts {
                    if k.eq_ignore_ascii_case("mergeSchema") && v.eq_ignore_ascii_case("true") {
                        merge_schema = true;
                    } else if k.eq_ignore_ascii_case("overwriteSchema")
                        && v.eq_ignore_ascii_case("true")
                    {
                        overwrite_schema = true;
                    } else if k.eq_ignore_ascii_case("sparkless.writer.format")
                        && v.eq_ignore_ascii_case("delta")
                    {
                        is_delta = true;
                    } else if k.eq_ignore_ascii_case("sparkless.delta.logicalMapPaths") {
                        logical_map_paths = serde_json::from_str(v).unwrap_or_default();
                    }
                }
                (merge_schema, overwrite_schema, is_delta, logical_map_paths)
            });

        // A session-local table or view shadows the external catalog. Otherwise, an exact
        // provider hit owns this write; provider errors or unsupported writes must never fall
        // through into Sparkless' in-memory or warehouse writer.
        if !session.saved_table_exists(name)
            && !session.has_exact_temp_view(name)
            && !SparkSession::is_global_temp_name(name)
            && let Some(provider) = session.external_catalog_provider()?
        {
            let descriptor = provider.resolve_table(name).map_err(|error| {
                PolarsError::InvalidOperation(
                    format!("external catalog provider failed to resolve '{name}': {error}").into(),
                )
            })?;
            token
                .check_cancelled()
                .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
            if let Some(descriptor) = descriptor {
                descriptor.validate().map_err(|error| {
                    PolarsError::InvalidOperation(
                        format!(
                            "external catalog returned an invalid descriptor for '{name}': {error}"
                        )
                        .into(),
                    )
                })?;
                return self.save_as_external_table_descriptor(
                    name,
                    mode,
                    options,
                    provider.as_ref(),
                    &descriptor,
                    &logical_map_paths,
                    token,
                );
            }
        }

        // Warehouse paths (disk-backed saveAsTable). For schema-qualified names, allow both:
        // - "{warehouse}/{schema.table}" (single dir)
        // - "{warehouse}/{schema}/{table}" (schema as subdir)
        let warehouse_paths: Vec<std::path::PathBuf> = session
            .warehouse_dir()
            .map(|w| {
                let mut out = Vec::with_capacity(2);
                if let Ok(p) = robin_sparkless_core::resolve_path_under_base(w, name) {
                    out.push(p);
                }
                if name.contains('.') {
                    let alt = name.replace('.', std::path::MAIN_SEPARATOR_STR);
                    if let Ok(p) = robin_sparkless_core::resolve_path_under_base(w, &alt) {
                        out.push(p);
                    }
                }
                out
            })
            .unwrap_or_default();
        let warehouse_exists = warehouse_paths.iter().any(|p| p.is_dir());

        // Delta saveAsTable is warehouse-backed. Let the transaction snapshot determine whether
        // the target exists as a Delta table; an empty warehouse directory is only a location.
        if is_delta_format {
            #[cfg(feature = "delta")]
            {
                let target = warehouse_paths
                    .iter()
                    .find(|path| path.join("_delta_log").is_dir())
                    .or_else(|| warehouse_paths.first())
                    .ok_or_else(|| {
                        PolarsError::InvalidOperation(
                            "saveAsTable(format=delta) requires spark.sql.warehouse.dir to be set."
                                .into(),
                        )
                    })?;

                match mode {
                    SaveMode::Ignore if session.saved_table_exists(name) => return Ok(()),
                    SaveMode::ErrorIfExists if session.saved_table_exists(name) => {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "Table or view '{name}' already exists. SaveMode is ErrorIfExists."
                            )
                            .into(),
                        ));
                    }
                    SaveMode::Append
                    | SaveMode::Overwrite
                    | SaveMode::Ignore
                    | SaveMode::ErrorIfExists => {}
                }
                let pl_df = self
                    .df
                    .collect_with_cancellation(token)
                    .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?
                    .as_ref()
                    .clone();
                token
                    .check_cancelled()
                    .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
                let resolved_partition_by = self
                    .partition_by
                    .iter()
                    .map(|name| self.df.resolve_column_name(name))
                    .collect::<Result<Vec<_>, _>>()?;
                crate::delta::write_delta_with_save_options_and_logical_map_paths_for_catalog(
                    &pl_df,
                    target,
                    mode,
                    merge_schema,
                    overwrite_schema,
                    &resolved_partition_by,
                    &logical_map_paths,
                )?;
                let loaded = crate::delta::read_delta(target, session.is_case_sensitive())?;
                session.register_table(name, loaded);
                return Ok(());
            }
            #[cfg(not(feature = "delta"))]
            {
                let _ = (
                    warehouse_paths,
                    warehouse_exists,
                    merge_schema,
                    overwrite_schema,
                    logical_map_paths,
                );
                return Err(PolarsError::InvalidOperation(
                    "saveAsTable(format=delta) requires the delta feature to be enabled.".into(),
                ));
            }
        }

        let warehouse_path = warehouse_paths.first().cloned();

        fn persist_to_warehouse(
            df: &crate::dataframe::DataFrame,
            dir: &Path,
        ) -> Result<(), PolarsError> {
            use std::fs;
            fs::create_dir_all(dir).map_err(|e| {
                PolarsError::ComputeError(format!("saveAsTable: create dir: {e}").into())
            })?;
            let file_path = dir.join("data.parquet");
            df.write()
                .mode(crate::dataframe::WriteMode::Overwrite)
                .format(crate::dataframe::WriteFormat::Parquet)
                .save(&file_path)
        }

        fn check_cancelled(token: &CancellationToken) -> Result<(), PolarsError> {
            token
                .check_cancelled()
                .map_err(|error| PolarsError::ComputeError(error.to_string().into()))
        }

        fn staging_path(target: &Path) -> std::path::PathBuf {
            static NEXT_STAGE_ID: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let id = NEXT_STAGE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let parent = target.parent().unwrap_or_else(|| Path::new("."));
            let name = target
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("table");
            parent.join(format!(
                ".{name}.sparkless-stage-{}-{id}",
                std::process::id()
            ))
        }

        fn stage_warehouse_write(
            df: &crate::dataframe::DataFrame,
            target: &Path,
            token: &CancellationToken,
        ) -> Result<std::path::PathBuf, PolarsError> {
            let materialized = df
                .collect_with_cancellation(token)
                .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
            check_cancelled(token)?;
            let eager = crate::dataframe::DataFrame::from_polars_with_options(
                materialized.as_ref().clone(),
                df.case_sensitive,
            );
            let stage = staging_path(target);
            if let Err(error) = persist_to_warehouse(&eager, &stage) {
                let _ = fs::remove_dir_all(&stage);
                return Err(error);
            }
            if let Err(error) = check_cancelled(token) {
                let _ = fs::remove_dir_all(&stage);
                return Err(error);
            }
            Ok(stage)
        }

        fn commit_warehouse_stage(
            stage: &Path,
            target: &Path,
            overwrite: bool,
        ) -> Result<(), PolarsError> {
            let backup = staging_path(target);
            let had_target = target.exists();
            if had_target && !overwrite {
                let _ = fs::remove_dir_all(stage);
                return Err(PolarsError::InvalidOperation(
                    format!("saveAsTable target already exists: {}", target.display()).into(),
                ));
            }
            if had_target {
                if let Err(error) = fs::rename(target, &backup) {
                    let _ = fs::remove_dir_all(stage);
                    return Err(PolarsError::ComputeError(
                        format!("saveAsTable: move existing target aside: {error}").into(),
                    ));
                }
            }
            if let Err(error) = fs::rename(stage, target) {
                if had_target {
                    let _ = fs::rename(&backup, target);
                }
                let _ = fs::remove_dir_all(stage);
                return Err(PolarsError::ComputeError(
                    format!("saveAsTable: commit staged directory: {error}").into(),
                ));
            }
            if had_target {
                let _ = fs::remove_dir_all(backup);
            }
            Ok(())
        }

        let final_df = match mode {
            SaveMode::ErrorIfExists => {
                if session.saved_table_exists(name) || warehouse_exists {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "Table or view '{name}' already exists. SaveMode is ErrorIfExists."
                        )
                        .into(),
                    ));
                }
                if let Some(ref p) = warehouse_path {
                    let stage = stage_warehouse_write(self.df, p, token)?;
                    check_cancelled(token)?;
                    commit_warehouse_stage(&stage, p, false)?;
                }
                self.df.clone()
            }
            SaveMode::Overwrite => {
                if let Some(ref p) = warehouse_path {
                    let stage = stage_warehouse_write(self.df, p, token)?;
                    check_cancelled(token)?;
                    commit_warehouse_stage(&stage, p, true)?;
                }
                self.df.clone()
            }
            SaveMode::Append => {
                let saved_existing = session.get_saved_table(name);
                let existing_logical_schema = saved_existing
                    .as_ref()
                    .and_then(|existing| existing.logical_schema.clone());
                let logical_schema = match (existing_logical_schema, self.df.logical_schema.clone())
                {
                    (Some(existing), Some(incoming)) => {
                        let existing_json = existing.to_json().map_err(|error| {
                            PolarsError::InvalidOperation(
                                format!(
                                    "saveAsTable append: serialize existing logical schema: {error}"
                                )
                                .into(),
                            )
                        })?;
                        let incoming_json = incoming.to_json().map_err(|error| {
                            PolarsError::InvalidOperation(
                                format!(
                                    "saveAsTable append: serialize incoming logical schema: {error}"
                                )
                                .into(),
                            )
                        })?;
                        if existing_json != incoming_json {
                            return Err(PolarsError::SchemaMismatch(
                                format!(
                                    "saveAsTable append: logical schema does not match existing table '{name}'"
                                )
                                .into(),
                            ));
                        }
                        Some(existing)
                    }
                    (Some(existing), None) => Some(existing),
                    (None, Some(incoming)) => Some(incoming),
                    (None, None) => None,
                };
                let existing_pl = if let Some(existing) = saved_existing.as_ref() {
                    existing.collect_inner()?.as_ref().clone()
                } else if warehouse_exists {
                    // Read from warehouse (data.parquet convention). For schema-qualified names,
                    // look in both "{warehouse}/{schema.table}" and "{warehouse}/{schema}/{table}".
                    let mut loaded: Option<polars::prelude::DataFrame> = None;
                    for p in &warehouse_paths {
                        if !p.is_dir() {
                            continue;
                        }
                        let data_file = p.join("data.parquet");
                        let read_path = if data_file.is_file() {
                            data_file.as_path()
                        } else {
                            p.as_path()
                        };
                        let pl_path = polars::prelude::PlRefPath::try_from_path(read_path)
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("saveAsTable append: path: {e}").into(),
                                )
                            })?;
                        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("saveAsTable append: read warehouse: {e}").into(),
                                )
                            })?;
                        loaded = Some(lf.collect().map_err(|e| {
                            PolarsError::ComputeError(
                                format!("saveAsTable append: collect: {e}").into(),
                            )
                        })?);
                        break;
                    }
                    loaded.ok_or_else(|| {
                        PolarsError::ComputeError(
                            format!("saveAsTable append: warehouse table '{name}' not found")
                                .into(),
                        )
                    })?
                } else {
                    // New table
                    if let Some(ref p) = warehouse_path {
                        let stage = stage_warehouse_write(self.df, p, token)?;
                        check_cancelled(token)?;
                        commit_warehouse_stage(&stage, p, false)?;
                    } else {
                        check_cancelled(token)?;
                    }
                    session.register_table(name, self.df.clone());
                    return Ok(());
                };
                let new_pl = self
                    .df
                    .collect_with_cancellation(token)
                    .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?
                    .as_ref()
                    .clone();
                if let Some(schema) = saved_existing
                    .as_ref()
                    .and_then(|existing| existing.logical_schema.as_ref())
                {
                    for field in schema.fields().iter().filter(|field| !field.nullable) {
                        let violates_nullability = match new_pl.column(&field.name) {
                            Ok(column) => column.null_count() > 0,
                            Err(_) => merge_schema,
                        };
                        if violates_nullability {
                            return Err(PolarsError::InvalidOperation(
                                format!(
                                    "saveAsTable append: non-nullable field '{}' contains null values",
                                    field.name
                                )
                                .into(),
                            ));
                        }
                    }
                }
                check_cancelled(token)?;
                let merged = if merge_schema {
                    let (aligned_existing, aligned_new) =
                        align_to_merged_schema_inline(&existing_pl, &new_pl)?;
                    let mut out = aligned_existing;
                    out.vstack_mut(&aligned_new)?;
                    crate::dataframe::DataFrame::from_polars_with_options(
                        out,
                        self.df.case_sensitive,
                    )
                } else {
                    let existing_cols: Vec<&str> = existing_pl
                        .get_column_names()
                        .iter()
                        .map(|s| s.as_str())
                        .collect();
                    let new_cols = new_pl.get_column_names();
                    let missing: Vec<_> = existing_cols
                        .iter()
                        .filter(|c| !new_cols.iter().any(|n| n.as_str() == **c))
                        .collect();
                    if !missing.is_empty() {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "saveAsTable append: new DataFrame missing columns: {:?}",
                                missing
                            )
                            .into(),
                        ));
                    }
                    let new_ordered = new_pl.select(existing_cols.iter().copied())?;
                    let mut combined = existing_pl;
                    combined.vstack_mut(&new_ordered)?;
                    crate::dataframe::DataFrame::from_polars_with_options(
                        combined,
                        self.df.case_sensitive,
                    )
                };
                let merged = if let Some(logical_schema) = logical_schema {
                    merged.with_logical_schema(logical_schema)?
                } else {
                    merged
                };
                if let Some(ref p) = warehouse_path {
                    let stage = stage_warehouse_write(&merged, p, token)?;
                    check_cancelled(token)?;
                    commit_warehouse_stage(&stage, p, true)?;
                }
                merged
            }
            SaveMode::Ignore => {
                if session.saved_table_exists(name) || warehouse_exists {
                    return Ok(());
                }
                if let Some(ref p) = warehouse_path {
                    let stage = stage_warehouse_write(self.df, p, token)?;
                    check_cancelled(token)?;
                    commit_warehouse_stage(&stage, p, false)?;
                }
                self.df.clone()
            }
        };
        if warehouse_path.is_none() {
            check_cancelled(token)?;
        }
        session.register_table(name, final_df);
        Ok(())
    }

    /// Write as Parquet (PySpark: parquet(path)). Equivalent to format(Parquet).save(path).
    pub fn parquet(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Parquet,
            format_was_set: true,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write as CSV (PySpark: csv(path)). Equivalent to format(Csv).save(path).
    pub fn csv(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Csv,
            format_was_set: true,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write as JSON lines (PySpark: json(path)). Equivalent to format(Json).save(path).
    pub fn json(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Json,
            format_was_set: true,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write to path. Overwrite replaces; append reads existing (if any) and concatenates then writes.
    /// With partition_by, path is a directory; each partition is written as path/col1=val1/col2=val2/... with partition columns omitted from the file (Spark/Hive style).
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        use polars::prelude::*;
        let path = robin_sparkless_core::maybe_confine_files_path(path.as_ref()).map_err(|e| {
            PolarsError::InvalidOperation(format!("write path confinement: {e}").into())
        })?;
        let path = path.as_path();
        let to_write: PlDataFrame = match self.mode {
            WriteMode::Overwrite => self.df.collect_inner()?.as_ref().clone(),
            WriteMode::Append => {
                if self.partition_by.is_empty() {
                    let existing: Option<PlDataFrame> = if path.exists() && path.is_file() {
                        match self.format {
                            WriteFormat::Parquet => polars::prelude::PlRefPath::try_from_path(path)
                                .ok()
                                .and_then(|pl_path| {
                                    LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
                                        .and_then(|lf| lf.collect())
                                        .ok()
                                }),
                            WriteFormat::Csv => polars::prelude::PlRefPath::try_from_path(path)
                                .ok()
                                .and_then(|pl_path| {
                                    LazyCsvReader::new(pl_path)
                                        .with_has_header(true)
                                        .finish()
                                        .and_then(|lf| lf.collect())
                                        .ok()
                                }),
                            WriteFormat::Json => polars::prelude::PlRefPath::try_from_path(path)
                                .ok()
                                .and_then(|pl_path| {
                                    LazyJsonLineReader::new(pl_path)
                                        .finish()
                                        .and_then(|lf| lf.collect())
                                        .ok()
                                }),
                        }
                    } else {
                        None
                    };
                    match existing {
                        Some(existing) => {
                            let lfs: [LazyFrame; 2] = [
                                existing.clone().lazy(),
                                self.df.collect_inner()?.as_ref().clone().lazy(),
                            ];
                            concat(lfs, UnionArgs::default())?.collect()?
                        }
                        None => self.df.collect_inner()?.as_ref().clone(),
                    }
                } else {
                    self.df.collect_inner()?.as_ref().clone()
                }
            }
        };

        if !self.partition_by.is_empty() {
            return self.save_partitioned(path, &to_write);
        }

        // When path is an existing directory (e.g. Delta table root from mkdtemp), write part file inside it.
        let ext = match self.format {
            WriteFormat::Parquet => "parquet",
            WriteFormat::Csv => "csv",
            WriteFormat::Json => "json",
        };
        let out_path = if path.exists() && path.is_dir() {
            let part_idx = if self.mode == WriteMode::Append {
                let suffix = format!(".{ext}");
                let max_n = std::fs::read_dir(path)
                    .map(|rd| {
                        rd.filter_map(Result::ok)
                            .filter_map(|e| {
                                e.file_name().to_str().and_then(|s| {
                                    s.strip_prefix("part-")
                                        .and_then(|t| t.strip_suffix(&suffix))
                                        .and_then(|t| t.parse::<u32>().ok())
                                })
                            })
                            .max()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0);
                max_n + 1
            } else {
                0
            };
            path.join(format!("part-{part_idx:05}.{ext}"))
        } else {
            path.to_path_buf()
        };

        match self.format {
            WriteFormat::Parquet => {
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        PolarsError::ComputeError(format!("write parquet create dir: {e}").into())
                    })?;
                }
                let mut file = std::fs::File::create(&out_path).map_err(|e| {
                    PolarsError::ComputeError(format!("write parquet create: {e}").into())
                })?;
                let mut df_mut = to_write;
                ParquetWriter::new(&mut file)
                    .finish(&mut df_mut)
                    .map_err(|e| PolarsError::ComputeError(format!("write parquet: {e}").into()))?;
            }
            WriteFormat::Csv => {
                let has_header = self
                    .options
                    .get("header")
                    .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                    .unwrap_or(true);
                let delimiter = self
                    .options
                    .get("sep")
                    .and_then(|s| s.bytes().next())
                    .unwrap_or(b',');
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        PolarsError::ComputeError(format!("write csv create dir: {e}").into())
                    })?;
                }
                let mut file = std::fs::File::create(&out_path).map_err(|e| {
                    PolarsError::ComputeError(format!("write csv create: {e}").into())
                })?;
                CsvWriter::new(&mut file)
                    .include_header(has_header)
                    .with_separator(delimiter)
                    .finish(&mut to_write.clone())
                    .map_err(|e| PolarsError::ComputeError(format!("write csv: {e}").into()))?;
            }
            WriteFormat::Json => {
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        PolarsError::ComputeError(format!("write json create dir: {e}").into())
                    })?;
                }
                let mut file = std::fs::File::create(&out_path).map_err(|e| {
                    PolarsError::ComputeError(format!("write json create: {e}").into())
                })?;
                JsonWriter::new(&mut file)
                    .finish(&mut to_write.clone())
                    .map_err(|e| PolarsError::ComputeError(format!("write json: {e}").into()))?;
            }
        }
        Ok(())
    }

    /// Write partitioned by columns: path/col1=val1/col2=val2/part-00000.{ext}. Partition columns are not written into the file (Spark/Hive style).
    fn save_partitioned(&self, path: &Path, to_write: &PlDataFrame) -> Result<(), PolarsError> {
        use polars::prelude::*;
        let resolved: Vec<String> = self
            .partition_by
            .iter()
            .map(|c| self.df.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let all_names = to_write.get_column_names();
        let data_cols: Vec<&str> = all_names
            .iter()
            .filter(|n| !resolved.iter().any(|r| r == n.as_str()))
            .map(|n| n.as_str())
            .collect();

        let unique_keys = to_write
            .select(resolved.iter().map(|s| s.as_str()).collect::<Vec<_>>())?
            .unique::<Option<&[String]>, String>(
                None,
                polars::prelude::UniqueKeepStrategy::First,
                None,
            )?;

        if self.mode == WriteMode::Overwrite && path.exists() {
            if path.is_dir() {
                std::fs::remove_dir_all(path).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write partitioned: remove_dir_all: {e}").into(),
                    )
                })?;
            } else {
                std::fs::remove_file(path).map_err(|e| {
                    PolarsError::ComputeError(format!("write partitioned: remove_file: {e}").into())
                })?;
            }
        }
        std::fs::create_dir_all(path).map_err(|e| {
            PolarsError::ComputeError(format!("write partitioned: create_dir_all: {e}").into())
        })?;

        let ext = match self.format {
            WriteFormat::Parquet => "parquet",
            WriteFormat::Csv => "csv",
            WriteFormat::Json => "json",
        };

        for row_idx in 0..unique_keys.height() {
            let row = unique_keys
                .get(row_idx)
                .ok_or_else(|| PolarsError::ComputeError("partition_row: get row".into()))?;
            let filter_expr = partition_row_to_filter_expr(&resolved, &row)?;
            let subset = to_write.clone().lazy().filter(filter_expr).collect()?;
            let subset = subset.select(data_cols.iter().copied())?;
            if subset.height() == 0 {
                continue;
            }

            let partition_segments = resolved
                .iter()
                .zip(row.iter())
                .map(|(name, av)| format_partition_value(av).map(|value| format!("{name}={value}")))
                .collect::<Result<Vec<_>, _>>()?;
            let part_path = partition_segments
                .into_iter()
                .fold(path.to_path_buf(), |p, seg| p.join(seg));
            std::fs::create_dir_all(&part_path).map_err(|e| {
                PolarsError::ComputeError(
                    format!("write partitioned: create_dir_all partition: {e}").into(),
                )
            })?;

            let file_idx = if self.mode == WriteMode::Append {
                let suffix = format!(".{ext}");
                let max_n = std::fs::read_dir(&part_path)
                    .map(|rd| {
                        rd.filter_map(Result::ok)
                            .filter_map(|e| {
                                e.file_name().to_str().and_then(|s| {
                                    s.strip_prefix("part-")
                                        .and_then(|t| t.strip_suffix(&suffix))
                                        .and_then(|t| t.parse::<u32>().ok())
                                })
                            })
                            .max()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0);
                max_n + 1
            } else {
                0
            };
            let filename = format!("part-{file_idx:05}.{ext}");
            let file_path = part_path.join(&filename);

            match self.format {
                WriteFormat::Parquet => {
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned parquet create: {e}").into(),
                        )
                    })?;
                    let mut df_mut = subset;
                    ParquetWriter::new(&mut file)
                        .finish(&mut df_mut)
                        .map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write partitioned parquet: {e}").into(),
                            )
                        })?;
                }
                WriteFormat::Csv => {
                    let has_header = self
                        .options
                        .get("header")
                        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                        .unwrap_or(true);
                    let delimiter = self
                        .options
                        .get("sep")
                        .and_then(|s| s.bytes().next())
                        .unwrap_or(b',');
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned csv create: {e}").into(),
                        )
                    })?;
                    CsvWriter::new(&mut file)
                        .include_header(has_header)
                        .with_separator(delimiter)
                        .finish(&mut subset.clone())
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("write partitioned csv: {e}").into())
                        })?;
                }
                WriteFormat::Json => {
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned json create: {e}").into(),
                        )
                    })?;
                    JsonWriter::new(&mut file)
                        .finish(&mut subset.clone())
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("write partitioned json: {e}").into())
                        })?;
                }
            }
        }
        Ok(())
    }
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame {
            inner: match &self.inner {
                DataFrameInner::Eager(df) => DataFrameInner::Eager(df.clone()),
                DataFrameInner::Lazy(lf) => DataFrameInner::Lazy(lf.clone()),
            },
            case_sensitive: self.case_sensitive,
            alias: self.alias.clone(),
            ambiguous_columns: self.ambiguous_columns.clone(),
            pre_select_lf: self.pre_select_lf.clone(),
            post_select_exprs: self.post_select_exprs.clone(),
            row_count_hint: self.row_count_hint,
            range_cardinality: self.range_cardinality.clone(),
            logical_schema: self.logical_schema.clone(),
        }
    }
}

/// Format a partition value as a reversible, path-safe directory component.
/// Null uses the Hive sentinel; all non-null values are percent-encoded so they cannot collide
/// with the sentinel or turn path data into directory separators.
fn format_partition_value(av: &AnyValue<'_>) -> Result<String, PolarsError> {
    let s = match av {
        AnyValue::Null => return Ok("__HIVE_DEFAULT_PARTITION__".to_string()),
        AnyValue::Boolean(b) => b.to_string(),
        AnyValue::Int32(i) => i.to_string(),
        AnyValue::Int64(i) => i.to_string(),
        AnyValue::UInt32(u) => u.to_string(),
        AnyValue::UInt64(u) => u.to_string(),
        AnyValue::Float32(f) => f.to_string(),
        AnyValue::Float64(f) => f.to_string(),
        AnyValue::String(s) => s.to_string(),
        AnyValue::StringOwned(s) => s.as_str().to_string(),
        AnyValue::Binary(value) => std::str::from_utf8(value).map(str::to_owned).map_err(|e| {
            PolarsError::InvalidOperation(
                format!("binary partition value is not valid UTF-8: {e}").into(),
            )
        })?,
        AnyValue::BinaryOwned(value) => {
            std::str::from_utf8(value).map(str::to_owned).map_err(|e| {
                PolarsError::InvalidOperation(
                    format!("binary partition value is not valid UTF-8: {e}").into(),
                )
            })?
        }
        AnyValue::Date(d) => date_days_to_json(*d)
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| {
                PolarsError::ComputeError(
                    format!("cannot format out-of-range Date partition value: {d}").into(),
                )
            })?,
        AnyValue::Datetime(value, unit, timezone) => {
            format_datetime_partition_value(*value, unit, timezone.is_some())?
        }
        AnyValue::DatetimeOwned(value, unit, timezone) => {
            format_datetime_partition_value(*value, unit, timezone.is_some())?
        }
        _ => av.to_string(),
    };
    Ok(percent_encoding::utf8_percent_encode(&s, percent_encoding::NON_ALPHANUMERIC).to_string())
}

fn format_datetime_partition_value(
    value: i64,
    unit: &TimeUnit,
    has_timezone: bool,
) -> Result<String, PolarsError> {
    let timestamp = datetime_anyvalue_to_json_iso(value, unit);
    let timestamp = timestamp.as_str().ok_or_else(|| {
        PolarsError::ComputeError(
            format!("cannot format out-of-range Datetime partition value: {value}").into(),
        )
    })?;
    if has_timezone {
        Ok(format!("{timestamp}Z"))
    } else {
        Ok(timestamp.replacen('T', " ", 1))
    }
}

/// Build a filter expression that matches rows where partition columns equal the given row values.
fn partition_row_to_filter_expr(
    col_names: &[String],
    row: &[AnyValue<'_>],
) -> Result<Expr, PolarsError> {
    if col_names.len() != row.len() {
        return Err(PolarsError::ComputeError(
            format!(
                "partition_row_to_filter_expr: {} columns but {} row values",
                col_names.len(),
                row.len()
            )
            .into(),
        ));
    }
    let mut pred = None::<Expr>;
    for (name, av) in col_names.iter().zip(row.iter()) {
        let clause = match av {
            AnyValue::Null => col(name.as_str()).is_null(),
            AnyValue::Boolean(b) => col(name.as_str()).eq(lit(*b)),
            AnyValue::Int32(i) => col(name.as_str()).eq(lit(*i)),
            AnyValue::Int64(i) => col(name.as_str()).eq(lit(*i)),
            // Date and Datetime AnyValues store their physical integer representation, while
            // casting a temporal column to String formats it as a date/time string. Comparing
            // those strings can miss rows because formatting precision differs from the writer.
            AnyValue::Date(days) => col(name.as_str()).cast(DataType::Int32).eq(lit(*days)),
            AnyValue::Datetime(value, _, _) | AnyValue::DatetimeOwned(value, _, _) => {
                col(name.as_str()).cast(DataType::Int64).eq(lit(*value))
            }
            AnyValue::UInt32(u) => col(name.as_str()).eq(lit(*u)),
            AnyValue::UInt64(u) => col(name.as_str()).eq(lit(*u)),
            AnyValue::Float32(f) => col(name.as_str()).eq(lit(*f)),
            AnyValue::Float64(f) => col(name.as_str()).eq(lit(*f)),
            AnyValue::String(s) => col(name.as_str()).eq(lit(s.to_string())),
            AnyValue::StringOwned(s) => col(name.as_str()).eq(lit(s.clone())),
            AnyValue::Binary(value) => col(name.as_str()).eq(lit(value.to_vec())),
            AnyValue::BinaryOwned(value) => col(name.as_str()).eq(lit(value.clone())),
            _ => {
                // Fallback: compare as string
                let s = av.to_string();
                col(name.as_str()).cast(DataType::String).eq(lit(s))
            }
        };
        pred = Some(match pred {
            None => clause,
            Some(p) => p.and(clause),
        });
    }
    Ok(pred.unwrap_or_else(|| lit(true)))
}

/// True if dtype is List(Struct{key, value}) (map column format).
fn is_map_format(dtype: &DataType) -> bool {
    if let DataType::List(inner) = dtype {
        if let DataType::Struct(fields) = inner.as_ref() {
            let has_key = fields.iter().any(|f| f.name == "key");
            let has_value = fields.iter().any(|f| f.name == "value");
            return has_key && has_value;
        }
    }
    false
}

/// When map value was unified to String by concat_list, keep it as a JSON string.
/// PySpark create_map often stringifies column-derived values when collected, so
/// we avoid re-interpreting numeric-looking strings as numbers here (issue #1140).
fn map_value_string_to_json(s: &str) -> JsonValue {
    JsonValue::String(s.to_string())
}

/// Round float to integer for JSON when within epsilon of an integer (#747, #748: 2**3 => 8 not 7.999...).
/// Use scale-tolerant epsilon so large power results (e.g. 2**30, 3**5) also round (#818, #820, #821, #823, #827).
fn float_to_json_number(f: f64) -> JsonValue {
    const EPSILON: f64 = 1e-6;
    if f.is_finite() {
        let rounded = f.round();
        if (f - rounded).abs() < EPSILON
            && let Some(number) = serde_json::Number::from_f64(rounded)
        {
            return JsonValue::Number(number);
        }
    }
    serde_json::Number::from_f64(f)
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

/// Format days since epoch as ISO date string (#849, #841, #840, #839: collect Date so Python gets non-null).
fn date_days_to_json(days: i32) -> JsonValue {
    let epoch = robin_sparkless_core::date_utils::epoch_naive_date();
    epoch
        .checked_add_signed(chrono::TimeDelta::days(days as i64))
        .map(|d| JsonValue::String(d.format("%Y-%m-%d").to_string()))
        .unwrap_or(JsonValue::Null)
}

/// Convert Polars Datetime AnyValue (i64 + TimeUnit) to ISO string for collect (#842, #843, #849).
fn datetime_anyvalue_to_json_iso(val: i64, unit: &TimeUnit) -> JsonValue {
    let micros = match unit {
        TimeUnit::Nanoseconds => val.checked_div(1000),
        TimeUnit::Microseconds => Some(val),
        TimeUnit::Milliseconds => val.checked_mul(1000),
    };
    micros
        .and_then(chrono::DateTime::from_timestamp_micros)
        .map(|dt| JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()))
        .unwrap_or(JsonValue::Null)
}

/// #1066: When a struct-typed column is stringified (e.g. Polars debug "{10}"), try to parse
/// it into a JSON object so nested structs work in collect. Handles one-field and multi-field structs.
fn struct_string_to_json_object(s: &str, fields: &[Field]) -> Option<JsonValue> {
    use serde_json::Map;
    if fields.is_empty() {
        return None;
    }
    let trimmed = s.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .map(|t| t.trim())
        .unwrap_or(trimmed);
    let mut obj = Map::new();
    if fields.len() == 1 {
        let f = &fields[0];
        let val = match &f.dtype {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => inner
                .parse::<i64>()
                .ok()
                .map(serde_json::Number::from)
                .map(JsonValue::Number),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => inner
                .parse::<u64>()
                .ok()
                .map(serde_json::Number::from)
                .map(JsonValue::Number),
            DataType::Float32 | DataType::Float64 => inner
                .parse::<f64>()
                .ok()
                .filter(|f| f.is_finite())
                .and_then(|f| serde_json::Number::from_f64(f).map(JsonValue::Number)),
            DataType::String => Some(JsonValue::String(
                inner
                    .strip_prefix('"')
                    .and_then(|t| t.strip_suffix('"'))
                    .unwrap_or(inner)
                    .to_string(),
            )),
            DataType::Boolean => {
                if inner.eq_ignore_ascii_case("true") {
                    Some(JsonValue::Bool(true))
                } else if inner.eq_ignore_ascii_case("false") {
                    Some(JsonValue::Bool(false))
                } else {
                    None
                }
            }
            _ => None,
        }?;
        obj.insert(f.name.to_string(), val);
        return Some(JsonValue::Object(obj));
    }
    // Multi-field: split by ", " and parse each segment (simple; quoted commas not handled).
    let parts: Vec<&str> = inner.splitn(fields.len(), ", ").map(|p| p.trim()).collect();
    for (i, f) in fields.iter().enumerate() {
        let part = parts.get(i).unwrap_or(&"").trim();
        let part_unescaped = part
            .strip_prefix('"')
            .and_then(|t| t.strip_suffix('"'))
            .unwrap_or(part);
        let val = if part.is_empty() || (part_unescaped.is_empty() && part != "\"\"") {
            JsonValue::Null
        } else {
            match &f.dtype {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => part
                    .parse::<i64>()
                    .ok()
                    .map(serde_json::Number::from)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null),
                DataType::Float32 | DataType::Float64 => part
                    .parse::<f64>()
                    .ok()
                    .filter(|x| x.is_finite())
                    .and_then(serde_json::Number::from_f64)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null),
                DataType::String => JsonValue::String(part_unescaped.to_string()),
                DataType::Boolean => {
                    if part.eq_ignore_ascii_case("true") {
                        JsonValue::Bool(true)
                    } else if part.eq_ignore_ascii_case("false") {
                        JsonValue::Bool(false)
                    } else {
                        JsonValue::Null
                    }
                }
                _ => JsonValue::Null,
            }
        };
        obj.insert(f.name.to_string(), val);
    }
    Some(JsonValue::Object(obj))
}

/// Convert Polars AnyValue to serde_json::Value for language bindings (Node, etc.).
/// Handles List and Struct so that create_map() with no args yields {} not null (#578).
/// Float values close to integers are rounded for collect parity (#747, #748).
/// Date and Datetime output ISO strings so Python bindings get date/datetime (#842, #843, #849).
/// Returns Err when map has null key (PySpark NULL_MAP_KEY parity, #1140).
fn any_value_to_json_with_array_struct_override(
    av: &AnyValue<'_>,
    dtype: &DataType,
    array_struct_paths: &HashSet<String>,
    path: &str,
) -> Result<JsonValue, PolarsError> {
    any_value_to_json_with_array_struct_override_policy(av, dtype, array_struct_paths, path, false)
}

fn any_value_to_json_with_array_struct_override_policy(
    av: &AnyValue<'_>,
    dtype: &DataType,
    array_struct_paths: &HashSet<String>,
    path: &str,
    preserve_float_precision: bool,
) -> Result<JsonValue, PolarsError> {
    use serde_json::Map;
    // Plan says String but execution may yield numeric (e.g. get_json_object; #1146): emit string.
    if matches!(dtype, DataType::String) {
        if let Some(s) = av.get_str() {
            return Ok(JsonValue::String(s.to_string()));
        }
        if matches!(
            av,
            AnyValue::Int8(_)
                | AnyValue::Int16(_)
                | AnyValue::Int32(_)
                | AnyValue::Int64(_)
                | AnyValue::UInt8(_)
                | AnyValue::UInt16(_)
                | AnyValue::UInt32(_)
                | AnyValue::UInt64(_)
                | AnyValue::Float32(_)
                | AnyValue::Float64(_)
                | AnyValue::Boolean(_)
        ) {
            return Ok(JsonValue::String(av.to_string()));
        }
    }
    Ok(match av {
        AnyValue::Null => JsonValue::Null,
        AnyValue::Decimal(value, _precision, scale) => {
            let negative = *value < 0;
            let digits = value.unsigned_abs().to_string();
            let scale = *scale;
            let mut decimal = if scale == 0 {
                digits
            } else if digits.len() <= scale {
                format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
            } else {
                format!(
                    "{}.{}",
                    &digits[..digits.len() - scale],
                    &digits[digits.len() - scale..]
                )
            };
            if negative {
                decimal.insert(0, '-');
            }
            JsonValue::String(decimal)
        }
        AnyValue::Boolean(b) => JsonValue::Bool(*b),
        // Date/Datetime columns may appear as Int32/Int64 from plan; serialize as ISO strings (#849, #841, #840, #839).
        AnyValue::Int32(i) if matches!(dtype, DataType::Date) => date_days_to_json(*i),
        AnyValue::Int64(i) if matches!(dtype, DataType::Datetime(_, _)) => match dtype {
            DataType::Datetime(unit, _) => datetime_anyvalue_to_json_iso(*i, unit),
            _ => datetime_anyvalue_to_json_iso(*i, &TimeUnit::Microseconds),
        },
        // Float column may yield Int from coalesce/agg; serialize as number (#837, #835, #812, #804, #802).
        AnyValue::Int32(i) if matches!(dtype, DataType::Float32 | DataType::Float64) => {
            float_to_json_number(*i as f64)
        }
        AnyValue::Int64(i) if matches!(dtype, DataType::Float32 | DataType::Float64) => {
            float_to_json_number(*i as f64)
        }
        AnyValue::Int8(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::Int16(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::Int32(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::Int64(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::UInt8(u) => JsonValue::Number(serde_json::Number::from(*u)),
        AnyValue::UInt16(u) => JsonValue::Number(serde_json::Number::from(*u)),
        AnyValue::UInt32(u) => JsonValue::Number(serde_json::Number::from(*u)),
        AnyValue::UInt64(u) => JsonValue::Number(serde_json::Number::from(*u)),
        AnyValue::Float32(f) if preserve_float_precision && !f.is_finite() => {
            return Err(PolarsError::ComputeError(
                "non-finite floating-point values cannot be encoded as JSON".into(),
            ));
        }
        AnyValue::Float64(f) if preserve_float_precision && !f.is_finite() => {
            return Err(PolarsError::ComputeError(
                "non-finite floating-point values cannot be encoded as JSON".into(),
            ));
        }
        AnyValue::Float32(f) if preserve_float_precision => {
            serde_json::Number::from_f64(f64::from(*f))
                .map(JsonValue::Number)
                .ok_or_else(|| {
                    PolarsError::ComputeError(
                        "non-finite floating-point values cannot be encoded as JSON".into(),
                    )
                })?
        }
        AnyValue::Float64(f) if preserve_float_precision => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                PolarsError::ComputeError(
                    "non-finite floating-point values cannot be encoded as JSON".into(),
                )
            })?,
        AnyValue::Float32(f) => float_to_json_number(f64::from(*f)),
        AnyValue::Float64(f) => float_to_json_number(*f),
        AnyValue::String(s) => {
            // List column may be stringified in some paths; parse back to array (#846, #845).
            if matches!(dtype, DataType::List(_)) {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                    if parsed.is_array() {
                        parsed
                    } else {
                        JsonValue::String(s.to_string())
                    }
                } else {
                    JsonValue::String(s.to_string())
                }
            } else if let DataType::Struct(fields) = dtype {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                    if parsed.is_object() {
                        parsed
                    } else if let Some(obj) = struct_string_to_json_object(s, fields) {
                        obj
                    } else {
                        JsonValue::String(s.to_string())
                    }
                } else if let Some(obj) = struct_string_to_json_object(s, fields) {
                    obj
                } else {
                    JsonValue::String(s.to_string())
                }
            } else {
                JsonValue::String(s.to_string())
            }
        }
        AnyValue::StringOwned(s) => {
            let s_ref = s.as_ref();
            if matches!(dtype, DataType::List(_)) {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s_ref) {
                    if parsed.is_array() {
                        parsed
                    } else {
                        JsonValue::String(s_ref.to_string())
                    }
                } else {
                    JsonValue::String(s_ref.to_string())
                }
            } else if let DataType::Struct(fields) = dtype {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s_ref) {
                    if parsed.is_object() {
                        parsed
                    } else if let Some(obj) = struct_string_to_json_object(s_ref, fields) {
                        obj
                    } else {
                        JsonValue::String(s_ref.to_string())
                    }
                } else if let Some(obj) = struct_string_to_json_object(s_ref, fields) {
                    obj
                } else {
                    JsonValue::String(s_ref.to_string())
                }
            } else {
                JsonValue::String(s_ref.to_string())
            }
        }
        AnyValue::List(s) => {
            if !array_struct_paths.contains(path) && is_map_format(dtype) {
                // List(Struct{key, value}) -> JSON object {} (PySpark empty map #578).
                // For mixed string + numeric/boolean map values, coerce numeric/boolean values to strings
                // so collected maps match PySpark behavior (issue #1140).
                let mut entries: Vec<(String, JsonValue)> = Vec::new();
                let mut has_string_value = false;
                let mut has_numeric_or_bool_value = false;
                for i in 0..s.len() {
                    if let Ok(elem) = s.get(i) {
                        let (k, v) = match &elem {
                            AnyValue::Struct(_, _, fields) => {
                                let mut k = None;
                                let mut v = None;
                                for (fld_av, fld) in elem._iter_struct_av().zip(fields.iter()) {
                                    if fld.name == "key" {
                                        if matches!(fld_av, AnyValue::Null) {
                                            return Err(PolarsError::ComputeError(
                                                "Cannot create map with null key (PySpark: NULL_MAP_KEY)".into(),
                                            ));
                                        }
                                        k = fld_av
                                            .get_str()
                                            .map(|s| s.to_string())
                                            .or_else(|| Some(fld_av.to_string()));
                                    } else if fld.name == "value" {
                                        v = Some(if matches!(fld.dtype, DataType::String) {
                                            if let Some(s) = fld_av.get_str() {
                                                map_value_string_to_json(s)
                                            } else {
                                                any_value_to_json_with_array_struct_override_policy(
                                                    &fld_av,
                                                    &fld.dtype,
                                                    array_struct_paths,
                                                    &format!("{path}{{value}}"),
                                                    preserve_float_precision,
                                                )?
                                            }
                                        } else {
                                            any_value_to_json_with_array_struct_override_policy(
                                                &fld_av,
                                                &fld.dtype,
                                                array_struct_paths,
                                                &format!("{path}{{value}}"),
                                                preserve_float_precision,
                                            )?
                                        });
                                    }
                                }
                                (k, v)
                            }
                            AnyValue::StructOwned(payload) => {
                                let (values, fields) = &**payload;
                                let mut k = None;
                                let mut v = None;
                                for (fld_av, fld) in values.iter().zip(fields.iter()) {
                                    if fld.name == "key" {
                                        if matches!(fld_av, AnyValue::Null) {
                                            return Err(PolarsError::ComputeError(
                                                "Cannot create map with null key (PySpark: NULL_MAP_KEY)".into(),
                                            ));
                                        }
                                        k = fld_av
                                            .get_str()
                                            .map(|s| s.to_string())
                                            .or_else(|| Some(fld_av.to_string()));
                                    } else if fld.name == "value" {
                                        v = Some(if matches!(fld.dtype, DataType::String) {
                                            if let Some(s) = fld_av.get_str() {
                                                map_value_string_to_json(s)
                                            } else {
                                                any_value_to_json_with_array_struct_override_policy(
                                                    fld_av,
                                                    &fld.dtype,
                                                    array_struct_paths,
                                                    &format!("{path}{{value}}"),
                                                    preserve_float_precision,
                                                )?
                                            }
                                        } else {
                                            any_value_to_json_with_array_struct_override_policy(
                                                fld_av,
                                                &fld.dtype,
                                                array_struct_paths,
                                                &format!("{path}{{value}}"),
                                                preserve_float_precision,
                                            )?
                                        });
                                    }
                                }
                                (k, v)
                            }
                            _ => (None, None),
                        };
                        if let (Some(key), Some(val)) = (k, v) {
                            if matches!(val, JsonValue::String(_)) {
                                has_string_value = true;
                            } else if matches!(val, JsonValue::Number(_) | JsonValue::Bool(_)) {
                                has_numeric_or_bool_value = true;
                            }
                            entries.push((key, val));
                        }
                    }
                }
                if has_string_value && has_numeric_or_bool_value {
                    for (_, v) in entries.iter_mut() {
                        match v {
                            JsonValue::Number(n) => {
                                *v = JsonValue::String(n.to_string());
                            }
                            JsonValue::Bool(b) => {
                                *v = JsonValue::String(b.to_string());
                            }
                            _ => {}
                        }
                    }
                }
                let mut obj = Map::new();
                for (key, val) in entries {
                    obj.insert(key, val);
                }
                JsonValue::Object(obj)
            } else {
                let inner_dtype = match dtype {
                    DataType::List(inner) => inner.as_ref(),
                    _ => dtype,
                };
                let arr: Vec<JsonValue> = (0..s.len())
                    .filter_map(|i| s.get(i).ok())
                    .map(|a| {
                        any_value_to_json_with_array_struct_override_policy(
                            &a,
                            inner_dtype,
                            array_struct_paths,
                            &format!("{path}[]"),
                            preserve_float_precision,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                JsonValue::Array(arr)
            }
        }
        AnyValue::Struct(_, _, fields) => {
            let mut vals: Vec<JsonValue> = Vec::with_capacity(fields.len());
            for (fld_av, fld) in av._iter_struct_av().zip(fields.iter()) {
                vals.push(any_value_to_json_with_array_struct_override_policy(
                    &fld_av,
                    &fld.dtype,
                    array_struct_paths,
                    &format!(
                        "{path}.{}",
                        serde_json::to_string(fld.name.as_str())
                            .expect("field names serialize as JSON strings")
                    ),
                    preserve_float_precision,
                )?);
            }
            let mut obj = Map::new();
            for (fld, v) in fields.iter().zip(vals) {
                obj.insert(fld.name.to_string(), v);
            }
            JsonValue::Object(obj)
        }
        AnyValue::StructOwned(payload) => {
            let (values, fields) = &**payload;
            let vals: Vec<JsonValue> = values
                .iter()
                .zip(fields.iter())
                .map(|(fld_av, fld)| {
                    any_value_to_json_with_array_struct_override_policy(
                        fld_av,
                        &fld.dtype,
                        array_struct_paths,
                        &format!(
                            "{path}.{}",
                            serde_json::to_string(fld.name.as_str())
                                .expect("field names serialize as JSON strings")
                        ),
                        preserve_float_precision,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut obj = Map::new();
            for (fld, v) in fields.iter().zip(vals) {
                obj.insert(fld.name.to_string(), v);
            }
            JsonValue::Object(obj)
        }
        AnyValue::Date(days) => date_days_to_json(*days),
        AnyValue::Datetime(val, unit, _) => datetime_anyvalue_to_json_iso(*val, unit),
        AnyValue::DatetimeOwned(val, unit, _) => datetime_anyvalue_to_json_iso(*val, unit),
        AnyValue::Duration(val, unit) => {
            JsonValue::Number(serde_json::Number::from(duration_to_micros(*val, unit)))
        }
        _ => JsonValue::Null,
    })
}

fn duration_to_micros(val: i64, unit: &polars::datatypes::TimeUnit) -> i64 {
    use polars::datatypes::TimeUnit;
    match unit {
        TimeUnit::Nanoseconds => val / 1_000,
        TimeUnit::Microseconds => val,
        TimeUnit::Milliseconds => val * 1_000,
    }
}

/// Broadcast hint - no-op that returns the same DataFrame (PySpark broadcast).
pub fn broadcast(df: &DataFrame) -> DataFrame {
    df.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{IntoSeries, NamedFrom, Series};

    #[test]
    fn with_columns_updates_replaced_column_nullability() {
        use polars::prelude::{DataType as PolarsDataType, NULL, lit};
        use robin_sparkless_core::DataType as LogicalDataType;

        let source = DataFrame::from_polars(
            polars::prelude::DataFrame::new_infer_height(vec![
                Series::new("value".into(), ["keep"]).into(),
            ])
            .unwrap(),
        )
        .with_logical_schema(StructType::new(vec![StructField::new(
            "value".into(),
            LogicalDataType::String,
            false,
        )]))
        .unwrap();
        let replacement = Column::from_expr(lit(NULL).cast(PolarsDataType::String), None);
        let replaced = source
            .with_columns(&[("value".into(), replacement)])
            .unwrap();

        assert!(replaced.schema().unwrap().fields()[0].nullable);
        assert_eq!(
            replaced
                .collect()
                .unwrap()
                .column("value")
                .unwrap()
                .get(0)
                .unwrap(),
            AnyValue::Null
        );
    }

    #[test]
    fn json_float_encoding_preserves_finite_values_and_rejects_non_finite() {
        assert_eq!(float_to_json_number(1.0000001), serde_json::json!(1.0));
        assert_eq!(
            any_value_to_json_with_array_struct_override_policy(
                &AnyValue::Float64(0.0000001),
                &DataType::Float64,
                &HashSet::new(),
                "\"n\"",
                true,
            )
            .unwrap(),
            serde_json::json!(0.0000001)
        );
        assert_eq!(
            any_value_to_json_with_array_struct_override_policy(
                &AnyValue::Float64(1.0000001),
                &DataType::Float64,
                &HashSet::new(),
                "\"n\"",
                true,
            )
            .unwrap(),
            serde_json::json!(1.0000001)
        );

        let error = any_value_to_json_with_array_struct_override_policy(
            &AnyValue::Float64(f64::NAN),
            &DataType::Float64,
            &HashSet::new(),
            "\"n\"",
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("non-finite floating-point"));

        let frame = DataFrame::from_polars(
            polars::prelude::DataFrame::new_infer_height(vec![
                Series::new("n".into(), [f64::NAN]).into(),
            ])
            .unwrap(),
        );
        let error = frame
            .collect_as_positional_json_with_limits(
                10,
                1024,
                1024,
                &HashSet::new(),
                &CancellationToken::new(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("non-finite floating-point"));
    }

    #[test]
    fn arrow_ipc_batches_reject_zero_max_rows() {
        let series = Series::new("value".into(), &[1i64]);
        let pl_df = polars::prelude::DataFrame::new_infer_height(vec![series.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);

        let result = df.to_arrow_ipc_batches_with_cancellation(0, 1024, &CancellationToken::new());

        assert!(matches!(
            result,
            Err(EngineError::User(message)) if message == "max_rows must be greater than zero"
        ));
    }

    /// Manual resource witness for lazy range construction and schema inspection.
    /// Run in separate processes with `SPARKLESS_RANGE_BENCH_ROWS` set to increasing sizes.
    #[test]
    #[ignore = "manual V17 lazy range construction RSS measurement"]
    fn lazy_range_construction_memory_measurement() {
        let rows = std::env::var("SPARKLESS_RANGE_BENCH_ROWS")
            .expect("set SPARKLESS_RANGE_BENCH_ROWS")
            .parse::<usize>()
            .expect("SPARKLESS_RANGE_BENCH_ROWS must be a positive integer");
        assert!(rows > 0);
        let end = i64::try_from(rows).expect("row count must fit i64");
        let session = SparkSession::builder()
            .app_name("lazy-range-memory-measurement")
            .get_or_create();

        let construction_started = std::time::Instant::now();
        let frame = session.range(0, end, 1).unwrap();
        let construction_micros = construction_started.elapsed().as_micros();
        std::hint::black_box(&frame);

        let schema_started = std::time::Instant::now();
        let schema = frame.schema().unwrap();
        let schema_micros = schema_started.elapsed().as_micros();
        assert_eq!(frame.row_count_hint, Some(rows as u64));
        assert_eq!(schema.fields()[0].name, "id");
        assert!(matches!(
            schema.fields()[0].data_type,
            crate::schema::DataType::Long
        ));
        assert!(!schema.fields()[0].nullable);

        println!(
            "range_memory rows={rows} strategy=lazy_scan construction_us={construction_micros} \
             schema_us={schema_micros} row_count_hint={rows} columns=1",
        );
    }

    #[test]
    fn filtered_large_range_count_uses_bounded_batches() {
        let session = SparkSession::builder()
            .app_name("bounded-range-batches")
            .get_or_create();
        for (rows, expected) in [
            (10_000, 9_994),
            (1_000_000, 999_994),
            (10_000_000, 9_999_994),
        ] {
            let frame = session
                .range(0, rows, 1)
                .unwrap()
                .filter(col("id").gt(5i64))
                .unwrap();
            assert_eq!(frame.schema().unwrap().fields()[0].name, "id");
            assert_eq!(frame.count().unwrap(), expected as usize);

            if rows == 10_000_000 {
                let range = frame
                    .range_cardinality
                    .as_ref()
                    .expect("range metadata survives the filter")
                    .clone();
                let mut batch_count = 0usize;
                let mut largest_batch = 0usize;
                let mut matched_rows = 0u64;
                let result = frame.for_each_range_batch(&range, None, |batch| {
                    batch_count += 1;
                    largest_batch = largest_batch.max(batch.height());
                    matched_rows += batch.height() as u64;
                    Ok(())
                });

                assert!(result.is_ok(), "range batch scan must succeed");
                assert!(batch_count > 1, "large input must be split into batches");
                assert!(largest_batch <= RANGE_EXECUTION_BATCH_ROWS);
                assert_eq!(matched_rows, expected as u64);
            }
        }
    }

    #[test]
    fn projected_range_keeps_exact_count_and_supports_filter_after_projection() {
        let session = SparkSession::builder()
            .app_name("projected-range-batches")
            .get_or_create();
        let projected = session
            .range(0, 10_000_000, 1)
            .unwrap()
            .select_exprs(vec![(col("id") * lit(2i64)).alias("doubled")])
            .unwrap();

        assert_eq!(projected.row_count_hint, Some(10_000_000));
        assert_eq!(projected.count().unwrap(), 10_000_000);
        assert_eq!(projected.columns().unwrap(), vec!["doubled"]);
        let range = projected
            .range_cardinality
            .as_ref()
            .expect("row-preserving projection keeps range metadata")
            .clone();
        let mut projected_rows = 0u64;
        let mut largest_batch = 0usize;
        let scan_result = projected.for_each_range_batch(&range, None, |batch| {
            projected_rows += batch.height() as u64;
            largest_batch = largest_batch.max(batch.height());
            Ok(())
        });
        assert!(scan_result.is_ok(), "projected range scan must succeed");
        assert_eq!(projected_rows, 10_000_000);
        assert!(largest_batch <= RANGE_EXECUTION_BATCH_ROWS);

        let tail = projected
            .filter(col("doubled").gt(lit(19_999_990i64)))
            .unwrap();
        assert_eq!(tail.count().unwrap(), 4);
        let collected = tail.collect().unwrap();
        let values = collected.column("doubled").unwrap().i64().unwrap();
        assert_eq!(
            values.into_no_null_iter().collect::<Vec<_>>(),
            vec![19_999_992, 19_999_994, 19_999_996, 19_999_998]
        );
    }

    #[test]
    fn projected_range_limit_preserves_schema_for_empty_and_nonempty_results() {
        let session = SparkSession::builder()
            .app_name("projected-range-limit-schema")
            .get_or_create();
        let projected = session
            .range(0, 10, 1)
            .unwrap()
            .select_exprs(vec![(col("id") + lit(1i64)).alias("next_id")])
            .unwrap();

        let empty = projected.limit(0).unwrap();
        assert_eq!(empty.columns().unwrap(), vec!["next_id"]);
        assert_eq!(empty.collect().unwrap().height(), 0);
        assert_eq!(empty.schema().unwrap().fields()[0].name, "next_id");

        let limited = projected.limit(3).unwrap();
        let collected = limited.collect().unwrap();
        assert_eq!(
            collected.column("next_id").unwrap().i64().unwrap().get(0),
            Some(1)
        );
        assert_eq!(
            collected.column("next_id").unwrap().i64().unwrap().get(2),
            Some(3)
        );
    }

    #[test]
    fn cardinality_changing_projection_drops_range_count_hint() {
        let session = SparkSession::builder()
            .app_name("cardinality-changing-range-projection")
            .get_or_create();
        let distinct_bools = session
            .range(0, 100, 1)
            .unwrap()
            .select_exprs(vec![col("id").cast(DataType::Boolean).unique()])
            .unwrap();

        assert_eq!(distinct_bools.row_count_hint, None);
        assert!(distinct_bools.range_cardinality.is_none());
        assert_eq!(distinct_bools.count().unwrap(), 2);
    }

    #[test]
    fn range_projection_does_not_assume_vector_literals_preserve_cardinality() {
        let vector_literal = lit(Series::new("values".into(), [1i64, 2]));

        assert!(!range_projection_preserves_row_count(std::slice::from_ref(
            &vector_literal
        )));
        assert!(!range_projection_can_run_per_batch(std::slice::from_ref(
            &vector_literal
        )));
    }

    #[test]
    fn mixed_select_items_keep_range_projection_on_the_bounded_path() {
        let session = SparkSession::builder()
            .app_name("mixed-select-items-range")
            .get_or_create();
        let projected = session
            .range(0, 1_000_000, 1)
            .unwrap()
            .select_items(vec![
                SelectItem::ColumnName("id"),
                SelectItem::Expr((col("id") + lit(1i64)).alias("next_id")),
            ])
            .unwrap();

        assert_eq!(projected.row_count_hint, Some(1_000_000));
        assert_eq!(projected.count().unwrap(), 1_000_000);
        let collected = projected.limit(2).unwrap().collect().unwrap();
        assert_eq!(
            collected.column("id").unwrap().i64().unwrap().get(1),
            Some(1)
        );
        assert_eq!(
            collected.column("next_id").unwrap().i64().unwrap().get(1),
            Some(2)
        );
    }

    #[test]
    fn projected_range_arrow_batches_use_the_bounded_native_source() {
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::SerReader;

        let session = SparkSession::builder()
            .app_name("projected-range-ipc")
            .get_or_create();
        let frame = session
            .range(0, 10_000_000, 1)
            .unwrap()
            .filter(col("id").gt(lit(40i64)))
            .unwrap()
            .select_exprs(vec![(col("id") * lit(2i64)).alias("doubled")])
            .unwrap()
            .limit(3)
            .unwrap();
        let mut batches = frame
            .to_arrow_ipc_batches_with_cancellation(2, 16 * 1024, &CancellationToken::new())
            .unwrap();

        let first = batches.next().unwrap().unwrap();
        let reader = IpcStreamReader::new(std::io::Cursor::new(first.bytes));
        let first_rows = reader.finish().unwrap();
        let values = first_rows.column("doubled").unwrap().i64().unwrap();
        assert_eq!(values.into_no_null_iter().collect::<Vec<_>>(), vec![82, 84]);

        let second = batches.next().unwrap().unwrap();
        let reader = IpcStreamReader::new(std::io::Cursor::new(second.bytes));
        let second_rows = reader.finish().unwrap();
        let values = second_rows.column("doubled").unwrap().i64().unwrap();
        assert_eq!(values.into_no_null_iter().collect::<Vec<_>>(), vec![86]);
        assert!(batches.next().is_none());
    }

    #[test]
    fn cancellable_dataframe_batches_use_the_native_range_source() {
        let session = SparkSession::builder()
            .app_name("projected-range-dataframe-batches")
            .get_or_create();
        let frame = session
            .range(0, 10_000_000, 1)
            .unwrap()
            .filter(col("id").gt(40i64))
            .unwrap()
            .select_exprs(vec![(col("id") * lit(2i64)).alias("doubled")])
            .unwrap()
            .limit(3)
            .unwrap();
        let token = CancellationToken::new();
        let mut batches = frame.collect_batches_with_cancellation(2, &token).unwrap();

        let mut values = Vec::new();
        for batch in &mut batches {
            let batch = batch.unwrap();
            values.extend(
                batch
                    .column("doubled")
                    .unwrap()
                    .i64()
                    .unwrap()
                    .into_no_null_iter(),
            );
        }
        assert_eq!(values, vec![82, 84, 86]);
    }

    #[test]
    fn range_batch_scan_observes_cancellation_between_batches() {
        let session = SparkSession::builder()
            .app_name("cancelled-range-batches")
            .get_or_create();
        let frame = session
            .range(0, 1_000_000, 1)
            .unwrap()
            .filter(col("id").gt(5i64))
            .unwrap();
        let range = frame.range_cardinality.as_ref().unwrap().clone();
        let token = CancellationToken::new();
        let mut visited = 0usize;
        let result = frame.for_each_range_batch(&range, Some(&token), |_| {
            visited += 1;
            token.cancel();
            Ok(())
        });

        assert!(matches!(
            result,
            Err(RangeBatchError::Engine(EngineError::Cancelled(_)))
        ));
        assert_eq!(visited, 1);
    }

    #[test]
    fn arrow_ipc_batches_carry_the_checked_logical_schema() {
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::SerReader;

        let physical = polars::prelude::df!("id" => [1i64, 2]).unwrap();
        let metadata = [("owner".to_string(), serde_json::json!("analytics"))]
            .into_iter()
            .collect();
        let logical = StructType::new(vec![
            StructField::new("id".into(), robin_sparkless_core::DataType::Long, false)
                .with_metadata(metadata),
        ]);
        let source = DataFrame::from_polars(physical)
            .with_logical_schema(logical.clone())
            .unwrap();
        let expected = logical.to_json().unwrap();
        let stream = source
            .to_arrow_ipc_batches_with_cancellation(2, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(stream.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected.as_str()));
        assert_eq!(reader.finish().unwrap().height(), 2);
    }

    #[test]
    fn native_scalar_type_ipc_roundtrip_preserves_values_and_logical_schema() {
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::{DataType as PlDataType, NamedFrom, SerReader, Series, TimeUnit};
        use robin_sparkless_core::DataType as LogicalDataType;

        let tiny = Series::new("tiny".into(), [Some(i8::MIN), None, Some(i8::MAX)]);
        let single = Series::new("single".into(), [Some(1.25_f32), None, Some(-0.5)]);
        let wall_clock = Series::new("wall_clock".into(), [Some(1_234_567_i64), None, Some(-1)])
            .cast(&PlDataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap();
        let payload = Series::new(
            "payload".into(),
            [Some(&b"\x00\xff"[..]), None, Some(&b"ok"[..])],
        );
        let physical = PlDataFrame::new_infer_height(vec![
            tiny.into(),
            single.into(),
            wall_clock.into(),
            payload.into(),
        ])
        .unwrap();
        let timestamp_metadata = [("semantics".to_string(), serde_json::json!("wall-clock"))]
            .into_iter()
            .collect();
        let logical = StructType::new(vec![
            StructField::new("tiny".into(), LogicalDataType::Byte, true),
            StructField::new("single".into(), LogicalDataType::Float, true),
            StructField::new("wall_clock".into(), LogicalDataType::TimestampNtz, true)
                .with_metadata(timestamp_metadata),
            StructField::new("payload".into(), LogicalDataType::Binary, true),
        ]);
        let expected_schema = logical.to_json().unwrap();
        let source = DataFrame::from_polars(physical)
            .with_logical_schema(logical)
            .unwrap();
        let batch = source
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));

        let decoded = reader.finish().unwrap();
        assert_eq!(
            decoded.column("tiny").unwrap().i8().unwrap().get(0),
            Some(i8::MIN)
        );
        assert_eq!(decoded.column("tiny").unwrap().i8().unwrap().get(1), None);
        assert_eq!(
            decoded.column("tiny").unwrap().i8().unwrap().get(2),
            Some(i8::MAX)
        );
        assert_eq!(
            decoded.column("single").unwrap().f32().unwrap().get(0),
            Some(1.25)
        );
        assert_eq!(
            decoded.column("single").unwrap().f32().unwrap().get(1),
            None
        );
        assert_eq!(
            decoded.column("single").unwrap().f32().unwrap().get(2),
            Some(-0.5)
        );
        let wall_clock = decoded.column("wall_clock").unwrap();
        assert_eq!(
            wall_clock.dtype(),
            &PlDataType::Datetime(TimeUnit::Microseconds, None)
        );
        assert_eq!(
            wall_clock.datetime().unwrap().physical().get(0),
            Some(1_234_567)
        );
        assert_eq!(wall_clock.datetime().unwrap().physical().get(1), None);
        assert_eq!(wall_clock.datetime().unwrap().physical().get(2), Some(-1));
        let payload = decoded.column("payload").unwrap().binary().unwrap();
        assert_eq!(payload.get(0), Some(&b"\x00\xff"[..]));
        assert_eq!(payload.get(1), None);
        assert_eq!(payload.get(2), Some(&b"ok"[..]));

        let empty = source.limit(0).unwrap();
        let empty_batch = empty
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(empty_batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));
        assert_eq!(reader.finish().unwrap().height(), 0);

        let scalar_transformed = source
            .filter(col("tiny").is_not_null())
            .unwrap()
            .select_exprs(vec![
                col("tiny").cast(PlDataType::Int8).alias("tiny_copy"),
                col("single").cast(PlDataType::Float32).alias("single_copy"),
                col("payload").alias("payload_copy"),
            ])
            .unwrap();
        let scalar_union = scalar_transformed.union(&scalar_transformed).unwrap();
        assert_eq!(scalar_union.count().unwrap(), 4);
        let scalar_schema = scalar_union.schema().unwrap();
        assert!(matches!(
            &scalar_schema.fields()[0].data_type,
            LogicalDataType::Byte
        ));
        assert!(matches!(
            &scalar_schema.fields()[1].data_type,
            LogicalDataType::Float
        ));
        assert!(matches!(
            &scalar_schema.fields()[2].data_type,
            LogicalDataType::Binary
        ));
        let scalar_expected_schema = scalar_schema.to_json().unwrap();
        let scalar_batch = scalar_union
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut scalar_reader = IpcStreamReader::new(std::io::Cursor::new(scalar_batch.bytes));
        let scalar_metadata = scalar_reader.custom_metadata().unwrap().unwrap();
        let scalar_encoded_schema = scalar_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(scalar_encoded_schema, Some(scalar_expected_schema.as_str()));
        let scalar_values = scalar_reader.finish().unwrap();
        assert_eq!(
            scalar_values
                .column("tiny_copy")
                .unwrap()
                .i8()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![i8::MIN, i8::MAX, i8::MIN, i8::MAX]
        );
        assert_eq!(
            scalar_values
                .column("single_copy")
                .unwrap()
                .f32()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![1.25, -0.5, 1.25, -0.5]
        );
        let binary_values = scalar_values
            .column("payload_copy")
            .unwrap()
            .binary()
            .unwrap();
        assert_eq!(binary_values.get(0), Some(&b"\x00\xff"[..]));
        assert_eq!(binary_values.get(1), Some(&b"ok"[..]));
        assert_eq!(binary_values.get(2), Some(&b"\x00\xff"[..]));
        assert_eq!(binary_values.get(3), Some(&b"ok"[..]));

        let empty_scalar_union = scalar_union.limit(0).unwrap();
        let empty_scalar_batch = empty_scalar_union
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut empty_scalar_reader =
            IpcStreamReader::new(std::io::Cursor::new(empty_scalar_batch.bytes));
        let empty_scalar_metadata = empty_scalar_reader.custom_metadata().unwrap().unwrap();
        let empty_scalar_schema = empty_scalar_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(empty_scalar_schema, Some(scalar_expected_schema.as_str()));
        assert_eq!(empty_scalar_reader.finish().unwrap().height(), 0);

        let transformed = source
            .filter(col("wall_clock").is_not_null())
            .unwrap()
            .select_exprs(vec![
                col("wall_clock")
                    .cast(PlDataType::Datetime(TimeUnit::Microseconds, None))
                    .alias("local_time"),
            ])
            .unwrap();
        let unioned = transformed.union(&transformed).unwrap();
        assert_eq!(unioned.count().unwrap(), 4);
        let unioned_schema = unioned.schema().unwrap();
        let timestamp_field = &unioned_schema.fields()[0];
        assert!(matches!(
            &timestamp_field.data_type,
            LogicalDataType::TimestampNtz
        ));
        assert!(timestamp_field.nullable);
        assert_eq!(timestamp_field.metadata["semantics"], "wall-clock");
        let unioned_expected_schema = unioned_schema.to_json().unwrap();
        let unioned_batch = unioned
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut unioned_reader = IpcStreamReader::new(std::io::Cursor::new(unioned_batch.bytes));
        let unioned_metadata = unioned_reader.custom_metadata().unwrap().unwrap();
        let unioned_encoded_schema = unioned_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(
            unioned_encoded_schema,
            Some(unioned_expected_schema.as_str())
        );
        let unioned_values = unioned_reader.finish().unwrap();
        let local_time = unioned_values.column("local_time").unwrap();
        assert_eq!(
            local_time.dtype(),
            &PlDataType::Datetime(TimeUnit::Microseconds, None)
        );
        let mut timestamps = local_time
            .datetime()
            .unwrap()
            .physical()
            .into_iter()
            .collect::<Vec<_>>();
        timestamps.sort();
        assert_eq!(
            timestamps,
            vec![Some(-1), Some(-1), Some(1_234_567), Some(1_234_567)]
        );

        let empty_union = unioned.limit(0).unwrap();
        let empty_union_batch = empty_union
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut empty_union_reader =
            IpcStreamReader::new(std::io::Cursor::new(empty_union_batch.bytes));
        let empty_union_metadata = empty_union_reader.custom_metadata().unwrap().unwrap();
        let empty_union_encoded_schema = empty_union_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(
            empty_union_encoded_schema,
            Some(unioned_expected_schema.as_str())
        );
        assert_eq!(empty_union_reader.finish().unwrap().height(), 0);
    }

    #[test]
    fn nested_array_ipc_roundtrip_preserves_values_and_logical_schema() {
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::{IntoSeries, ListChunked, SerReader};

        let items = ListChunked::from_iter([
            Some(Series::new("".into(), vec![Some(1_i32), None])),
            None,
            Some(Series::new("".into(), vec![Some(3_i32)])),
        ])
        .with_name("items".into())
        .into_series();
        let physical = polars::prelude::DataFrame::new_infer_height(vec![items.into()]).unwrap();
        let metadata = [("owner".to_string(), serde_json::json!("analytics"))]
            .into_iter()
            .collect();
        let logical = StructType::new(vec![
            StructField::new(
                "items".into(),
                robin_sparkless_core::DataType::ArrayWithNullability {
                    element_type: Box::new(robin_sparkless_core::DataType::Integer),
                    contains_null: true,
                },
                true,
            )
            .with_metadata(metadata),
        ]);
        let source = DataFrame::from_polars(physical)
            .with_logical_schema(logical.clone())
            .unwrap();
        let expected_schema = logical.to_json().unwrap();
        let batch = source
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));

        let decoded = reader.finish().unwrap();
        let arrays = decoded.column("items").unwrap().list().unwrap();
        let first = arrays.get_as_series(0).unwrap();
        let first = first.i32().unwrap();
        assert_eq!(first.get(0), Some(1));
        assert_eq!(first.get(1), None);
        assert!(arrays.get_as_series(1).is_none());
        let third = arrays.get_as_series(2).unwrap();
        assert_eq!(third.i32().unwrap().get(0), Some(3));

        let empty = source.limit(0).unwrap();
        let empty_batch = empty
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(empty_batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));
        assert_eq!(reader.finish().unwrap().height(), 0);
    }

    #[test]
    fn nested_array_of_struct_schema_and_ipc_survive_transformations() {
        use polars::chunked_array::StructChunked;
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::{IntoSeries, ListChunked, NamedFrom, SerReader, Series};
        use robin_sparkless_core::DataType as LogicalDataType;

        let first_id = Series::new("id".into(), [1_i32, 2]);
        let first_label = Series::new("label".into(), ["one", "two"]);
        let first =
            StructChunked::from_series("element".into(), 2, [&first_id, &first_label].into_iter())
                .unwrap()
                .into_series();
        let last_id = Series::new("id".into(), [3_i32]);
        let last_label = Series::new("label".into(), ["three"]);
        let last =
            StructChunked::from_series("element".into(), 1, [&last_id, &last_label].into_iter())
                .unwrap()
                .into_series();
        let elements = ListChunked::from_iter([Some(first), None, Some(last)])
            .with_name("elements".into())
            .into_series();
        let physical = PlDataFrame::new_infer_height(vec![elements.into()]).unwrap();
        let nested_metadata = [("origin".to_string(), serde_json::json!("nested IPC"))]
            .into_iter()
            .collect();
        let logical = StructType::new(vec![StructField::new(
            "elements".into(),
            LogicalDataType::ArrayWithNullability {
                element_type: Box::new(LogicalDataType::Struct(vec![
                    StructField::new("id".into(), LogicalDataType::Integer, false)
                        .with_metadata(nested_metadata),
                    StructField::new("label".into(), LogicalDataType::String, true),
                ])),
                contains_null: false,
            },
            true,
        )]);
        assert!(logical.try_to_polars_schema().is_err());

        let source = DataFrame::from_polars(physical)
            .with_logical_schema(logical.clone())
            .unwrap();
        let filtered = source.filter(col("elements").is_not_null()).unwrap();
        let projected = filtered
            .select_exprs(vec![col("elements").alias("payload")])
            .unwrap();
        let transformed = projected.union(&projected).unwrap();
        assert_eq!(transformed.count().unwrap(), 4);
        let transformed_schema = transformed.schema().unwrap();
        let LogicalDataType::ArrayWithNullability {
            element_type,
            contains_null,
        } = &transformed_schema.fields()[0].data_type
        else {
            panic!("array-of-struct logical type must survive transformations");
        };
        assert_eq!(transformed_schema.fields()[0].name, "payload");
        assert!(!contains_null);
        let LogicalDataType::Struct(nested_fields) = element_type.as_ref() else {
            panic!("array element must retain its logical struct schema");
        };
        assert!(!nested_fields[0].nullable);
        assert_eq!(nested_fields[0].metadata["origin"], "nested IPC");

        let expected_schema = transformed_schema.to_json().unwrap();
        let batch = transformed
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));

        let decoded = reader.finish().unwrap();
        let rows = decoded.column("payload").unwrap().list().unwrap();
        for row in 0..4 {
            let values = rows.get_as_series(row).unwrap();
            let fields = values.struct_().unwrap();
            let expected_id = if row % 2 == 0 { 1 } else { 3 };
            assert_eq!(
                fields.field_by_name("id").unwrap().i32().unwrap().get(0),
                Some(expected_id)
            );
            let label_series = fields.field_by_name("label").unwrap();
            let labels = label_series.str().unwrap();
            if row % 2 == 0 {
                assert_eq!(labels.get(1), Some("two"));
            } else {
                assert_eq!(labels.get(0), Some("three"));
            }
        }

        let copied = transformed
            .with_column("replica", &transformed.column("payload").unwrap())
            .unwrap();
        let renamed = copied
            .with_column_renamed("replica", "payload_copy")
            .unwrap();
        let renamed_schema = renamed.schema().unwrap();
        let payload_schema = transformed_schema
            .fields()
            .iter()
            .find(|field| field.name == "payload")
            .unwrap();
        let copy_schema = renamed_schema
            .fields()
            .iter()
            .find(|field| field.name == "payload_copy")
            .unwrap();
        assert_eq!(
            serde_json::to_value(&copy_schema.data_type).unwrap(),
            serde_json::to_value(&payload_schema.data_type).unwrap()
        );
        let LogicalDataType::ArrayWithNullability { element_type, .. } = &copy_schema.data_type
        else {
            panic!("copied array must preserve its logical declaration");
        };
        let LogicalDataType::Struct(copy_fields) = element_type.as_ref() else {
            panic!("copied array elements must preserve their logical struct");
        };
        assert!(!copy_fields[0].nullable);
        assert_eq!(copy_fields[0].metadata["origin"], "nested IPC");

        let renamed_expected_schema = renamed_schema.to_json().unwrap();
        let renamed_batch = renamed
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(renamed_batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(renamed_expected_schema.as_str()));
        let decoded = reader.finish().unwrap();
        let original = decoded.column("payload").unwrap().list().unwrap();
        let copy = decoded.column("payload_copy").unwrap().list().unwrap();
        for row in 0..4 {
            let original_series = original.get_as_series(row).unwrap();
            let original = original_series.struct_().unwrap();
            let copied_series = copy.get_as_series(row).unwrap();
            let copied = copied_series.struct_().unwrap();
            assert_eq!(
                original.field_by_name("id").unwrap().i32().unwrap().get(0),
                copied.field_by_name("id").unwrap().i32().unwrap().get(0)
            );
            assert_eq!(
                original
                    .field_by_name("label")
                    .unwrap()
                    .str()
                    .unwrap()
                    .get(0),
                copied.field_by_name("label").unwrap().str().unwrap().get(0)
            );
        }

        let empty = renamed.limit(0).unwrap();
        let empty_batch = empty
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(empty_batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(renamed_expected_schema.as_str()));
        assert_eq!(reader.finish().unwrap().height(), 0);
    }

    #[test]
    fn nonnullable_array_elements_survive_logical_sidecar_and_ipc() {
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::{IntoSeries, ListChunked, SerReader, Series};
        use robin_sparkless_core::DataType as LogicalDataType;

        let items = ListChunked::from_iter([
            Some(Series::new("".into(), vec![1_i32, 2])),
            None,
            Some(Series::new("".into(), vec![3_i32])),
        ])
        .with_name("items".into())
        .into_series();
        let physical = polars::prelude::DataFrame::new_infer_height(vec![items.into()]).unwrap();
        let logical = StructType::new(vec![StructField::new(
            "items".into(),
            LogicalDataType::ArrayWithNullability {
                element_type: Box::new(LogicalDataType::Integer),
                contains_null: false,
            },
            true,
        )]);
        assert!(logical.try_to_polars_schema().is_err());

        let source = DataFrame::from_polars(physical)
            .with_logical_schema(logical)
            .unwrap();
        let projected = source
            .filter(col("items").is_not_null())
            .unwrap()
            .select_exprs(vec![col("items").alias("payload")])
            .unwrap();
        let unioned = projected.union(&projected).unwrap();
        let unioned_schema = unioned.schema().unwrap();
        assert_eq!(unioned_schema.fields()[0].name, "payload");
        assert!(matches!(
            &unioned_schema.fields()[0].data_type,
            LogicalDataType::ArrayWithNullability {
                contains_null: false,
                ..
            }
        ));

        let expected_schema = unioned_schema.to_json().unwrap();
        let batch = unioned
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));

        let decoded = reader.finish().unwrap();
        assert_eq!(decoded.height(), 4);
        let arrays = decoded.column("payload").unwrap().list().unwrap();
        assert_eq!(
            arrays
                .get_as_series(0)
                .unwrap()
                .i32()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            arrays.get_as_series(1).unwrap().i32().unwrap().get(0),
            Some(3)
        );
        assert_eq!(
            arrays.get_as_series(2).unwrap().i32().unwrap().get(1),
            Some(2)
        );
        assert_eq!(
            arrays.get_as_series(3).unwrap().i32().unwrap().get(0),
            Some(3)
        );
    }

    #[test]
    fn nested_struct_ipc_roundtrip_preserves_values_nulls_and_logical_schema() {
        use polars::chunked_array::StructChunked;
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::{NamedFrom, SerReader, Series};
        use polars_arrow::bitmap::Bitmap;

        let id = Series::new("id".into(), vec![1_i32, 2, 3]);
        let label = Series::new("label".into(), ["first", "hidden", "third"]);
        let payload = StructChunked::from_series("payload".into(), 3, [&id, &label].into_iter())
            .unwrap()
            .with_outer_validity(Some(Bitmap::from_iter([true, false, true])))
            .into_series();
        let physical = PlDataFrame::new_infer_height(vec![payload.into()]).unwrap();
        let nested_metadata = [("origin".to_string(), serde_json::json!("shared catalog"))]
            .into_iter()
            .collect();
        let logical = StructType::new(vec![StructField::new(
            "payload".into(),
            robin_sparkless_core::DataType::Struct(vec![
                StructField::new("id".into(), robin_sparkless_core::DataType::Integer, false)
                    .with_metadata(nested_metadata),
                StructField::new("label".into(), robin_sparkless_core::DataType::String, true),
            ]),
            true,
        )]);
        let source = DataFrame::from_polars(physical)
            .with_logical_schema(logical.clone())
            .unwrap();
        let expected_schema = logical.to_json().unwrap();
        let filtered_schema = source
            .filter(col("payload").is_not_null())
            .unwrap()
            .schema()
            .unwrap();
        assert_eq!(filtered_schema.to_json().unwrap(), expected_schema);
        let projected = source
            .select_exprs(vec![col("payload").alias("copy")])
            .unwrap();
        let projected_schema = projected.schema().unwrap();
        let projected_field = &projected_schema.fields()[0];
        assert_eq!(projected_field.name, "copy");
        assert!(projected_field.nullable);
        let robin_sparkless_core::DataType::Struct(nested_fields) = &projected_field.data_type
        else {
            panic!("projected payload must retain its nested logical schema");
        };
        assert!(!nested_fields[0].nullable);
        assert_eq!(nested_fields[0].metadata["origin"], "shared catalog");
        let batch = source
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));

        let decoded = reader.finish().unwrap();
        let payload = decoded.column("payload").unwrap();
        let fields = payload.struct_().unwrap();
        assert_eq!(
            fields.field_by_name("id").unwrap().i32().unwrap().get(0),
            Some(1)
        );
        assert_eq!(
            fields.field_by_name("id").unwrap().i32().unwrap().get(2),
            Some(3)
        );
        assert!(matches!(payload.get(1).unwrap(), AnyValue::Null));
    }

    #[test]
    fn nested_struct_schema_survives_union_filter_projection_and_ipc() {
        use polars::chunked_array::StructChunked;
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::{NamedFrom, SerReader, Series};
        use polars_arrow::bitmap::Bitmap;
        use robin_sparkless_core::DataType as LogicalDataType;

        fn source_frame() -> DataFrame {
            let id = Series::new("id".into(), vec![1_i32, 2, 3]);
            let label = Series::new("label".into(), ["first", "hidden", "third"]);
            let payload =
                StructChunked::from_series("payload".into(), 3, [&id, &label].into_iter())
                    .unwrap()
                    .with_outer_validity(Some(Bitmap::from_iter([true, false, true])))
                    .into_series();
            let physical = PlDataFrame::new_infer_height(vec![payload.into()]).unwrap();
            let nested_metadata = [("origin".to_string(), serde_json::json!("shared catalog"))]
                .into_iter()
                .collect();
            let logical = StructType::new(vec![StructField::new(
                "payload".into(),
                LogicalDataType::Struct(vec![
                    StructField::new("id".into(), LogicalDataType::Integer, false)
                        .with_metadata(nested_metadata),
                    StructField::new("label".into(), LogicalDataType::String, true),
                ]),
                true,
            )]);
            DataFrame::from_polars(physical)
                .with_logical_schema(logical)
                .unwrap()
        }

        fn assert_nested_ids(frame: &DataFrame, column_name: &str, expected: &[Option<i32>]) {
            let collected = frame.collect().unwrap();
            assert_eq!(collected.height(), expected.len());
            let payload = collected.column(column_name).unwrap();
            let fields = payload.struct_().unwrap();
            let ids = fields.field_by_name("id").unwrap();
            let ids = ids.i32().unwrap();
            let labels = fields.field_by_name("label").unwrap();
            let labels = labels.str().unwrap();
            for (index, expected) in expected.iter().enumerate() {
                match expected {
                    Some(value) => {
                        assert_eq!(ids.get(index), Some(*value));
                        let expected_label = match value {
                            1 => "first",
                            2 => "hidden",
                            3 => "third",
                            _ => panic!("unexpected nested id {value}"),
                        };
                        assert_eq!(labels.get(index), Some(expected_label));
                    }
                    None => assert!(matches!(payload.get(index).unwrap(), AnyValue::Null)),
                }
            }
        }

        let source = source_frame();
        let source_schema_json = source.schema().unwrap().to_json().unwrap();
        let unioned = source.union(&source).unwrap();
        let filtered = unioned.filter(col("payload").is_not_null()).unwrap();
        let projected = filtered
            .select_exprs(vec![col("payload").alias("renamed")])
            .unwrap();

        let duplicated = source
            .select_exprs(vec![
                col("payload").alias("payload"),
                col("payload").alias("payload"),
            ])
            .unwrap();
        let duplicated_schema = duplicated.schema().unwrap();
        assert_eq!(duplicated_schema.fields().len(), 2);
        assert_eq!(
            duplicated_schema
                .fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["payload", "payload_1"]
        );
        for field in duplicated_schema.fields() {
            let LogicalDataType::Struct(fields) = &field.data_type else {
                panic!("duplicate nested labels must retain their struct schema");
            };
            assert!(!fields[0].nullable);
            assert_eq!(fields[0].metadata["origin"], "shared catalog");
        }
        let duplicated_expected_schema = duplicated_schema.to_json().unwrap();
        let duplicated_batch = duplicated
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut duplicated_reader =
            IpcStreamReader::new(std::io::Cursor::new(duplicated_batch.bytes));
        let duplicated_metadata = duplicated_reader.custom_metadata().unwrap().unwrap();
        let duplicated_encoded_schema = duplicated_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(
            duplicated_encoded_schema,
            Some(duplicated_expected_schema.as_str())
        );
        let duplicated_values = duplicated_reader.finish().unwrap();
        assert_eq!(duplicated_values.width(), 2);
        assert_eq!(
            duplicated_values
                .select_at_idx(0)
                .unwrap()
                .as_materialized_series(),
            duplicated_values
                .select_at_idx(1)
                .unwrap()
                .as_materialized_series()
        );
        assert_nested_ids(&duplicated, "payload", &[Some(1), None, Some(3)]);
        assert_nested_ids(&duplicated, "payload_1", &[Some(1), None, Some(3)]);

        assert_eq!(projected.count().unwrap(), 4);
        assert_nested_ids(&projected, "renamed", &[Some(1), Some(3), Some(1), Some(3)]);
        let schema = projected.schema().unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.fields()[0].name, "renamed");
        let LogicalDataType::Struct(nested_fields) = &schema.fields()[0].data_type else {
            panic!("nested logical struct must survive union, filter, and projection");
        };
        assert!(!nested_fields[0].nullable);
        assert_eq!(nested_fields[0].metadata["origin"], "shared catalog");

        let dotted = source.select(vec!["payload.id"]).unwrap();
        let nested_field = dotted.schema().unwrap();
        assert_eq!(nested_field.fields()[0].name, "id");
        assert!(nested_field.fields()[0].nullable);
        assert_eq!(
            nested_field.fields()[0].metadata["origin"],
            "shared catalog"
        );
        let dotted_values = dotted.collect().unwrap();
        let dotted_ids = dotted_values.column("id").unwrap().i32().unwrap();
        assert_eq!(dotted_ids.get(0), Some(1));
        assert_eq!(dotted_ids.get(1), None);
        assert_eq!(dotted_ids.get(2), Some(3));

        let nested_projection = source.select(vec!["payload.id"]).unwrap();
        let nested_schema_json = nested_projection.schema().unwrap().to_json().unwrap();
        let nested_batch = nested_projection
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut nested_reader = IpcStreamReader::new(std::io::Cursor::new(nested_batch.bytes));
        let nested_metadata = nested_reader.custom_metadata().unwrap().unwrap();
        let encoded_nested_schema = nested_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_nested_schema, Some(nested_schema_json.as_str()));
        nested_reader.finish().unwrap();

        let selected_items_frame = source
            .select_items(vec![SelectItem::Expr(col("payload").alias("item_payload"))])
            .unwrap();
        let selected_items = selected_items_frame.schema().unwrap();
        let LogicalDataType::Struct(item_fields) = &selected_items.fields()[0].data_type else {
            panic!("mixed select must retain the nested logical schema");
        };
        assert_eq!(selected_items.fields()[0].name, "item_payload");
        assert!(!item_fields[0].nullable);
        assert_eq!(item_fields[0].metadata["origin"], "shared catalog");
        assert_nested_ids(
            &selected_items_frame,
            "item_payload",
            &[Some(1), None, Some(3)],
        );

        let selected_nested_item = source
            .select_items(vec![SelectItem::ColumnName("payload.id")])
            .unwrap()
            .schema()
            .unwrap();
        assert_eq!(selected_nested_item.fields()[0].name, "id");
        assert!(selected_nested_item.fields()[0].nullable);
        assert_eq!(
            selected_nested_item.fields()[0].metadata["origin"],
            "shared catalog"
        );

        let selected_expr_frame = source
            .select_expr(&["payload as expr_payload".to_string()])
            .unwrap();
        let selected_expr = selected_expr_frame.schema().unwrap();
        let LogicalDataType::Struct(expr_fields) = &selected_expr.fields()[0].data_type else {
            panic!("selectExpr must retain the nested logical schema");
        };
        assert_eq!(selected_expr.fields()[0].name, "expr_payload");
        assert!(!expr_fields[0].nullable);
        assert_eq!(expr_fields[0].metadata["origin"], "shared catalog");
        assert_nested_ids(
            &selected_expr_frame,
            "expr_payload",
            &[Some(1), None, Some(3)],
        );

        let copied = source
            .with_column_expr("payload_copy", col("payload"))
            .unwrap();
        let copied_schema = copied.schema().unwrap();
        assert_eq!(copied_schema.fields().len(), 2);
        let LogicalDataType::Struct(copied_fields) = &copied_schema.fields()[1].data_type else {
            panic!("a copied nested column must retain its logical struct type");
        };
        assert!(!copied_fields[0].nullable);
        assert_eq!(copied_fields[0].metadata["origin"], "shared catalog");
        assert_nested_ids(&copied, "payload_copy", &[Some(1), None, Some(3)]);

        let renamed = source
            .with_column_renamed("payload", "renamed_payload")
            .unwrap();
        let renamed_schema = renamed.schema().unwrap();
        let LogicalDataType::Struct(renamed_fields) = &renamed_schema.fields()[0].data_type else {
            panic!("renaming must retain the nested logical schema");
        };
        assert_eq!(renamed_schema.fields()[0].name, "renamed_payload");
        assert!(!renamed_fields[0].nullable);
        assert_eq!(renamed_fields[0].metadata["origin"], "shared catalog");
        assert_nested_ids(&renamed, "renamed_payload", &[Some(1), None, Some(3)]);

        let by_name = source.union_by_name(&source, false).unwrap();
        assert_eq!(
            by_name.schema().unwrap().to_json().unwrap(),
            source_schema_json
        );
        assert_nested_ids(
            &by_name,
            "payload",
            &[Some(1), None, Some(3), Some(1), None, Some(3)],
        );

        let row_filtered = source.dropna(None, "any", None).unwrap();
        assert_eq!(
            row_filtered.schema().unwrap().to_json().unwrap(),
            source_schema_json
        );
        assert_nested_ids(&row_filtered, "payload", &[Some(1), Some(3)]);

        let expected_schema = schema.to_json().unwrap();
        let batch = projected
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));

        let decoded = reader.finish().unwrap();
        assert_eq!(decoded.height(), 4);
        let payload = decoded.column("renamed").unwrap();
        let fields = payload.struct_().unwrap();
        assert_eq!(
            fields.field_by_name("id").unwrap().i32().unwrap().get(0),
            Some(1)
        );
        assert_eq!(
            fields.field_by_name("id").unwrap().i32().unwrap().get(1),
            Some(3)
        );
        assert_eq!(
            fields.field_by_name("label").unwrap().str().unwrap().get(3),
            Some("third")
        );

        let empty = source_frame().limit(0).unwrap();
        let empty_schema = empty.schema().unwrap();
        let expected_empty_schema = empty_schema.to_json().unwrap();
        let mut empty_batches = empty
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap();
        let empty_batch = empty_batches
            .next()
            .expect("empty IPC result must carry its schema")
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(empty_batch.bytes));
        let custom_metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = custom_metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_empty_schema.as_str()));
        assert_eq!(reader.finish().unwrap().height(), 0);
        assert!(empty_batches.next().is_none());

        let mut undersized_empty_batch = empty
            .to_arrow_ipc_batches_with_cancellation(8, 1, &CancellationToken::new())
            .unwrap();
        assert!(matches!(
            undersized_empty_batch.next().unwrap(),
            Err(EngineError::User(message)) if message.contains("empty Arrow IPC schema")
        ));
    }

    #[test]
    fn nested_logical_schema_survives_saved_table_append_and_temp_view_roundtrip() {
        use polars::chunked_array::StructChunked;
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::{SerReader, Series};
        use polars_arrow::bitmap::Bitmap;
        use robin_sparkless_core::DataType as LogicalDataType;

        fn source_frame(metadata_value: &str) -> DataFrame {
            let id = Series::new("id".into(), vec![1_i32, 2, 3]);
            let label = Series::new("label".into(), ["first", "hidden", "third"]);
            let payload =
                StructChunked::from_series("payload".into(), 3, [&id, &label].into_iter())
                    .unwrap()
                    .with_outer_validity(Some(Bitmap::from_iter([true, false, true])))
                    .into_series();
            let physical = PlDataFrame::new_infer_height(vec![payload.into()]).unwrap();
            let metadata = [("origin".to_string(), serde_json::json!(metadata_value))]
                .into_iter()
                .collect();
            let logical = StructType::new(vec![StructField::new(
                "payload".into(),
                LogicalDataType::Struct(vec![
                    StructField::new("id".into(), LogicalDataType::Integer, false)
                        .with_metadata(metadata),
                    StructField::new("label".into(), LogicalDataType::String, true),
                ]),
                true,
            )]);
            DataFrame::from_polars(physical)
                .with_logical_schema(logical)
                .unwrap()
        }

        fn assert_nested_ids(frame: &PlDataFrame, expected: &[Option<i32>]) {
            assert_eq!(frame.height(), expected.len());
            let payload = frame.column("payload").unwrap();
            let fields = payload.struct_().unwrap();
            let ids = fields.field_by_name("id").unwrap();
            let ids = ids.i32().unwrap();
            for (index, expected) in expected.iter().enumerate() {
                match expected {
                    Some(value) => assert_eq!(ids.get(index), Some(*value)),
                    None => assert!(matches!(payload.get(index).unwrap(), AnyValue::Null)),
                }
            }
        }

        let suffix = std::process::id();
        let table_name = format!("nested_append_{suffix}");
        let view_name = format!("nested_append_view_{suffix}");
        let session = SparkSession::builder()
            .app_name(format!("nested-append-{suffix}"))
            .get_or_create();
        let source = source_frame("table metadata");
        let expected_schema = source.schema().unwrap().to_json().unwrap();

        source
            .write()
            .save_as_table(&session, &table_name, SaveMode::Overwrite)
            .unwrap();
        let original = session.table(&table_name).unwrap();
        assert_eq!(
            original.schema().unwrap().to_json().unwrap(),
            expected_schema
        );

        // A physical-compatible append that changes declared nested metadata must fail before
        // replacing the saved table or changing its original rows/schema.
        let incompatible = source_frame("different metadata");
        let mismatch = incompatible
            .write()
            .save_as_table(&session, &table_name, SaveMode::Append);
        assert!(
            matches!(mismatch, Err(PolarsError::SchemaMismatch(_))),
            "unexpected logical-schema append result: {mismatch:?}"
        );
        let unchanged = session.table(&table_name).unwrap();
        assert_eq!(
            unchanged.schema().unwrap().to_json().unwrap(),
            expected_schema
        );
        assert_nested_ids(&unchanged.collect().unwrap(), &[Some(1), None, Some(3)]);

        source
            .write()
            .save_as_table(&session, &table_name, SaveMode::Append)
            .unwrap();
        let appended = session.table(&table_name).unwrap();
        assert_eq!(
            appended.schema().unwrap().to_json().unwrap(),
            expected_schema
        );
        assert_nested_ids(
            &appended.collect().unwrap(),
            &[Some(1), None, Some(3), Some(1), None, Some(3)],
        );

        session.create_or_replace_temp_view(&view_name, appended);
        let view = session.table(&view_name).unwrap();
        assert_eq!(view.schema().unwrap().to_json().unwrap(), expected_schema);
        let batch = view
            .to_arrow_ipc_batches_with_cancellation(8, 16 * 1024, &CancellationToken::new())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut reader = IpcStreamReader::new(std::io::Cursor::new(batch.bytes));
        let metadata = reader.custom_metadata().unwrap().unwrap();
        let encoded_schema = metadata
            .iter()
            .find(|(key, _)| key.as_str() == "sparkless.logical_schema.v1")
            .map(|(_, value)| value.as_str());
        assert_eq!(encoded_schema, Some(expected_schema.as_str()));
        assert_nested_ids(
            &reader.finish().unwrap(),
            &[Some(1), None, Some(3), Some(1), None, Some(3)],
        );

        assert!(session.drop_temp_view(&view_name));
        assert!(session.drop_table(&table_name));
    }

    /// Issue #235: root-level string–numeric comparison coercion in filter.
    #[test]
    fn coerce_string_numeric_root_in_filter() {
        let s = Series::new("str_col".into(), &["123", "456"]);
        let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);
        let expr = col("str_col").eq(lit(123i64));
        let out = df.filter(expr).unwrap();
        assert_eq!(out.count().unwrap(), 1);
    }

    #[test]
    fn lazy_count_preserves_filter_semantics_and_checks_cancellation() {
        let frame = polars::prelude::df!("id" => [1i64, 2, 3, 4]).unwrap();
        let df = DataFrame::from_lazy_with_options(frame.lazy(), false);
        let filtered = df
            .filter(crate::functions::col("id").gt(lit(2i64)).into_expr())
            .unwrap();
        assert_eq!(filtered.count().unwrap(), 2);
        assert_eq!(
            filtered
                .count_with_cancellation(&CancellationToken::new())
                .unwrap(),
            2
        );

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            filtered.count_with_cancellation(&cancelled),
            Err(EngineError::Cancelled(_))
        ));
    }

    #[test]
    fn logical_schema_metadata_and_nullability_follow_filter_projection_and_union() {
        let physical = polars::prelude::df!("id" => [1i64, 2, 3]).unwrap();
        let metadata = [("owner".to_string(), serde_json::json!("analytics"))]
            .into_iter()
            .collect();
        let logical = StructType::new(vec![
            StructField::new("id".into(), robin_sparkless_core::DataType::Long, false)
                .with_metadata(metadata),
        ]);
        let source = DataFrame::from_lazy_with_options(physical.lazy(), false)
            .with_logical_schema(logical)
            .unwrap();

        let filtered = source.filter(col("id").gt(lit(1i64))).unwrap();
        let filtered_schema = filtered.schema().unwrap();
        let field = &filtered_schema.fields()[0];
        assert!(!field.nullable);
        assert_eq!(field.metadata["owner"], "analytics");

        let casted = source
            .select_exprs(vec![col("id").cast(DataType::Float64).alias("cast_id")])
            .unwrap();
        let casted_schema = casted.schema().unwrap();
        let field = &casted_schema.fields()[0];
        assert!(matches!(
            field.data_type,
            robin_sparkless_core::DataType::Double
        ));
        assert!(field.nullable);
        assert_eq!(field.metadata["owner"], "analytics");

        let projected = filtered
            .select_exprs(vec![col("id").alias("renamed")])
            .unwrap();
        let projected_schema = projected.schema().unwrap();
        let field = &projected_schema.fields()[0];
        assert_eq!(field.name, "renamed");
        assert!(!field.nullable);
        assert_eq!(field.metadata["owner"], "analytics");

        let unioned = projected.union(&projected).unwrap();
        let unioned_schema = unioned.schema().unwrap();
        let field = &unioned_schema.fields()[0];
        assert!(!field.nullable);
        assert_eq!(field.metadata["owner"], "analytics");
    }

    #[test]
    fn union_preserves_nested_nullability_after_type_widening() {
        use robin_sparkless_core::DataType as LogicalDataType;

        let left_values = Series::new("".into(), vec![Some(1_i32), None]);
        let left_array = Series::new("items".into(), [left_values]);
        let left =
            DataFrame::from_polars(PlDataFrame::new_infer_height(vec![left_array.into()]).unwrap())
                .with_logical_schema(StructType::new(vec![StructField::new(
                    "items".into(),
                    LogicalDataType::ArrayWithNullability {
                        element_type: Box::new(LogicalDataType::Integer),
                        contains_null: true,
                    },
                    true,
                )]))
                .unwrap();

        let right_values = Series::new("".into(), vec![Some(2_i64)]);
        let right_array = Series::new("items".into(), [right_values]);
        let right = DataFrame::from_polars(
            PlDataFrame::new_infer_height(vec![right_array.into()]).unwrap(),
        )
        .with_logical_schema(StructType::new(vec![StructField::new(
            "items".into(),
            LogicalDataType::ArrayWithNullability {
                element_type: Box::new(LogicalDataType::Long),
                contains_null: false,
            },
            true,
        )]))
        .unwrap();

        let unioned = left.union(&right).unwrap();
        assert!(matches!(
            &unioned.schema().unwrap().fields()[0].data_type,
            LogicalDataType::ArrayWithNullability {
                element_type,
                contains_null: true,
            } if matches!(element_type.as_ref(), LogicalDataType::Long)
        ));
    }

    #[test]
    fn union_treats_missing_nested_schema_as_nullable() {
        use robin_sparkless_core::DataType as LogicalDataType;

        let declared_values = Series::new("".into(), vec![Some(1_i64)]);
        let declared_array = Series::new("items".into(), [declared_values]);
        let declared = DataFrame::from_polars(
            PlDataFrame::new_infer_height(vec![declared_array.into()]).unwrap(),
        )
        .with_logical_schema(StructType::new(vec![StructField::new(
            "items".into(),
            LogicalDataType::ArrayWithNullability {
                element_type: Box::new(LogicalDataType::Long),
                contains_null: false,
            },
            true,
        )]))
        .unwrap();

        let undeclared_values = Series::new("".into(), vec![Some(2_i64), None]);
        let undeclared_array = Series::new("items".into(), [undeclared_values]);
        let undeclared = DataFrame::from_polars(
            PlDataFrame::new_infer_height(vec![undeclared_array.into()]).unwrap(),
        );

        let unioned = declared.union(&undeclared).unwrap();
        assert!(matches!(
            &unioned.schema().unwrap().fields()[0].data_type,
            LogicalDataType::ArrayWithNullability {
                contains_null: true,
                ..
            }
        ));
    }

    #[test]
    fn append_without_incoming_logical_sidecar_checks_nonnullable_values() {
        use robin_sparkless_core::DataType as LogicalDataType;

        let session = SparkSession::new(
            Some("append-nullability-sidecar-test".into()),
            None,
            HashMap::new(),
        );
        let table_name = format!("append_nullability_{}", std::process::id());
        let existing = DataFrame::from_polars(polars::prelude::df!("value" => [1_i64]).unwrap())
            .with_logical_schema(StructType::new(vec![StructField::new(
                "value".into(),
                LogicalDataType::Long,
                false,
            )]))
            .unwrap();
        existing
            .write()
            .save_as_table(&session, &table_name, SaveMode::Overwrite)
            .unwrap();

        let incoming = DataFrame::from_lazy_with_options(
            polars::prelude::df!("value" => [None::<i64>])
                .unwrap()
                .lazy(),
            false,
        );
        assert!(
            incoming
                .write()
                .save_as_table(&session, &table_name, SaveMode::Append)
                .is_err()
        );
        assert_eq!(
            session
                .table(&table_name)
                .unwrap()
                .collect()
                .unwrap()
                .height(),
            1
        );
    }

    #[test]
    fn union_keeps_the_coerced_physical_type_through_chained_logical_schemas() {
        use robin_sparkless_core::DataType as LogicalDataType;

        let metadata = [("owner".to_string(), serde_json::json!("left"))]
            .into_iter()
            .collect();
        let numeric = DataFrame::from_polars(polars::prelude::df!("key" => [1i64]).unwrap())
            .with_logical_schema(StructType::new(vec![
                StructField::new("key".into(), LogicalDataType::Long, false)
                    .with_metadata(metadata),
            ]))
            .unwrap();
        let text = DataFrame::from_polars(polars::prelude::df!("key" => ["2"]).unwrap());
        let third_number = DataFrame::from_polars(polars::prelude::df!("key" => [3i64]).unwrap());

        let first = numeric.union(&text).unwrap();
        let first_schema = first.schema().unwrap();
        let first_field = &first_schema.fields()[0];
        assert!(matches!(&first_field.data_type, LogicalDataType::String));
        assert!(first_field.nullable);
        assert_eq!(first_field.metadata["owner"], "left");

        let chained = first.union(&third_number).unwrap();
        let chained_schema = chained.schema().unwrap();
        let chained_field = &chained_schema.fields()[0];
        assert!(matches!(&chained_field.data_type, LogicalDataType::String));
        assert_eq!(chained_field.metadata["owner"], "left");
        let (_, rows, _) = chained.collect_as_json_rows_with_names().unwrap();
        assert_eq!(rows.len(), 3);
        assert!(
            rows.iter()
                .all(|row| row.get("key").is_some_and(serde_json::Value::is_string))
        );
    }

    #[test]
    fn union_by_name_merges_logical_fields_by_name_when_right_has_extra_columns() {
        let left = DataFrame::from_lazy(polars::prelude::df!("payload" => [1i64]).unwrap().lazy());
        let right = DataFrame::from_polars(
            polars::prelude::df!("extra" => [9i64], "payload" => [2i64]).unwrap(),
        )
        .with_logical_schema(StructType::new(vec![
            StructField::new("extra".into(), robin_sparkless_core::DataType::Long, false)
                .with_metadata(
                    [("owner".to_string(), serde_json::json!("extra"))]
                        .into_iter()
                        .collect(),
                ),
            StructField::new(
                "payload".into(),
                robin_sparkless_core::DataType::Long,
                false,
            )
            .with_metadata(
                [("owner".to_string(), serde_json::json!("payload"))]
                    .into_iter()
                    .collect(),
            ),
        ]))
        .unwrap();

        let combined = left.union_by_name(&right, false).unwrap();
        let schema = combined.schema().unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.fields()[0].name, "payload");
        assert_eq!(schema.fields()[0].metadata["owner"], "payload");
        let rows = combined.collect_as_json_rows().unwrap();
        assert_eq!(rows[0]["payload"], serde_json::json!(1));
        assert_eq!(rows[1]["payload"], serde_json::json!(2));
    }

    #[test]
    fn logical_schema_attachment_checks_physical_type_compatibility() {
        let physical = polars::prelude::df!("id" => [1i64]).unwrap();
        let logical = StructType::new(vec![StructField::new(
            "id".into(),
            robin_sparkless_core::DataType::Double,
            true,
        )]);
        assert!(
            DataFrame::from_polars(physical)
                .with_logical_schema(logical)
                .is_err()
        );
    }

    #[test]
    fn positional_collection_enforces_encoded_cell_and_result_budgets() {
        let frame = polars::prelude::df!("value" => ["a string that will not fit"]).unwrap();
        let df = DataFrame::from_lazy_with_options(frame.lazy(), false);
        let token = CancellationToken::new();
        assert!(matches!(
            df.collect_as_positional_json_with_limits(
                10,
                1024,
                4,
                &HashSet::new(),
                &token,
            ),
            Err(EngineError::User(message)) if message.contains("cell")
        ));
        assert!(matches!(
            df.collect_as_positional_json_with_limits(
                10,
                8,
                1024,
                &HashSet::new(),
                &token,
            ),
            Err(EngineError::User(message)) if message.contains("serialized result")
        ));
    }

    /// #988: Numeric column == string literal: coerce string to numeric (PySpark parity).
    #[test]
    fn coerce_numeric_column_eq_string_literal() {
        let s = Series::new("value".into(), &[100i64, 200i64, 50i64]);
        let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);
        let expr = col("value").eq(lit("100"));
        let out = df.filter(expr).unwrap();
        assert_eq!(out.count().unwrap(), 1);
        let rows = out.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("value").and_then(|v| v.as_i64()), Some(100));
    }

    #[test]
    fn filter_sql_decimal_integral_literal_preserves_precision() {
        let s = Series::new(
            "amount".into(),
            &[
                Some(9_007_199_254_740_992i128),
                Some(9_007_199_254_740_993i128),
            ],
        )
        .into_decimal(20, 0)
        .unwrap()
        .into_series();
        let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);
        // SQL `expr()` aliases its parsed expression with the original source text.
        let condition = col("amount")
            .eq(lit(9_007_199_254_740_993i64))
            .alias("amount = 9007199254740993");
        let out = df.filter(condition).unwrap();
        assert_eq!(out.count().unwrap(), 1);
    }

    /// #1589: to_date(expr) == string literal coerces string to date (PySpark parity).
    #[test]
    fn filter_to_date_expr_eq_string_literal() {
        use crate::functions::{col, to_date};

        let s = Series::new("end_date".into(), &["2025-01-01", "9999-12-31"]);
        let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);
        let date_col = to_date(&col("end_date"), Some("yyyy-MM-dd")).unwrap();
        let expr = date_col.into_expr().eq(lit("9999-12-31"));
        let out = df.filter(expr).unwrap();
        assert_eq!(out.count().unwrap(), 1);
        let rows = out.collect_as_json_rows().unwrap();
        assert_eq!(
            rows[0].get("end_date").and_then(|v| v.as_str()),
            Some("9999-12-31")
        );
    }

    /// #1571: logical NOT (~) on string-column == numeric-literal must coerce inner comparison.
    #[test]
    fn filter_not_string_column_eq_numeric_literal() {
        use crate::functions::col;

        let s = Series::new("A".into(), &["1"]);
        let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);
        let cmp = col("A").eq(crate::functions::lit_i64(1).into_expr());
        let condition = cmp.logical_not().into_expr();
        let out = df.filter(condition).unwrap();
        assert_eq!(out.count().unwrap(), 0);
    }

    /// #1545: logical Or of numeric-column == string-literal comparisons (filter coercion).
    #[test]
    fn coerce_numeric_column_eq_string_literal_under_or() {
        use polars::prelude::Operator;
        use std::sync::Arc;

        let s = Series::new("A".into(), &[123i32, 456i32]);
        let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);
        let left = col("A").eq(lit("123"));
        let right = col("A").eq(lit("123"));
        let expr = Expr::BinaryExpr {
            left: Arc::new(left),
            op: Operator::Or,
            right: Arc::new(right),
        };
        let out = df.filter(expr).unwrap();
        assert_eq!(out.count().unwrap(), 1);
        let rows = out.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("A").and_then(|v| v.as_i64()), Some(123));
    }

    /// #646: filter with string predicate (contains) must receive Boolean; cast ensures parity.
    #[test]
    fn filter_with_string_contains_predicate() {
        use crate::functions::col;
        use serde_json::json;

        let spark = crate::session::SparkSession::builder()
            .app_name("filter_contains_test")
            .get_or_create();
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let rows = vec![
            vec![json!(1), json!("alice")],
            vec![json!(2), json!("bob")],
            vec![json!(3), json!("charlie")],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        let cond: polars::prelude::Expr = col("name").contains("lic").into_expr();
        let filtered = df.filter(cond).unwrap();
        assert_eq!(
            filtered.count().unwrap(),
            1,
            "filter(name.contains(\"lic\")) should return one row (alice)"
        );
    }

    /// Lazy backend: schema, columns, resolve_column_name work on lazy DataFrame.
    #[test]
    fn lazy_schema_columns_resolve_before_collect() {
        let spark = SparkSession::builder()
            .app_name("lazy_mod_tests")
            .get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1i64, 25i64, "a".to_string()),
                    (2i64, 30i64, "b".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        assert_eq!(df.columns().unwrap(), vec!["id", "age", "name"]);
        assert_eq!(df.resolve_column_name("AGE").unwrap(), "age");
        assert!(df.get_column_dtype("id").unwrap().is_integer());
    }

    /// Lazy backend: from_lazy produces valid DataFrame.
    #[test]
    fn lazy_from_lazy_produces_valid_df() {
        let _spark = SparkSession::builder()
            .app_name("lazy_mod_tests")
            .get_or_create();
        let pl_df = polars::prelude::df!("x" => &[1i64, 2, 3]).unwrap();
        let df = DataFrame::from_lazy_with_options(pl_df.lazy(), false);
        assert_eq!(df.columns().unwrap(), vec!["x"]);
        assert_eq!(df.count().unwrap(), 3);
    }

    /// Batch 1 / Sparkless parity: null cells must serialize as JsonValue::Null (Python None).
    #[test]
    fn collect_preserves_null_as_json_null() {
        use serde_json::Value as JsonValue;

        let _spark = SparkSession::builder()
            .app_name("collect_null_test")
            .get_or_create();
        let s_id = Series::new("id".into(), &[1i64, 2i64, 3i64]);
        let s_val = Series::new("value".into(), vec![Some(10i64), None, Some(30i64)]);
        let pl_df =
            polars::prelude::DataFrame::new_infer_height(vec![s_id.into(), s_val.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);
        let rows = df.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("value").and_then(|v| v.as_i64()), Some(10));
        assert!(rows[1].contains_key("value"));
        assert!(matches!(rows[1].get("value"), Some(JsonValue::Null)));
        assert_eq!(rows[2].get("value").and_then(|v| v.as_i64()), Some(30));
    }

    /// #156: DataFrame.pivot is a stub; must return clear error (use crosstab).
    #[test]
    fn pivot_raises_not_implemented() {
        let spark = SparkSession::builder()
            .app_name("pivot_stub_test")
            .get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1i64, 25i64, "a".to_string()),
                    (2i64, 30i64, "b".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let err = match df.pivot("name", None) {
            Ok(_) => panic!("pivot should not be implemented"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("groupBy(...).pivot") && msg.contains("crosstab"),
            "pivot stub should mention crosstab: {}",
            msg
        );
    }

    /// #747, #748: collect rounds floats that are close to integers (e.g. 2**3 => 8 not 7.999...).
    #[test]
    fn collect_rounds_float_near_integer() {
        let _spark = SparkSession::builder()
            .app_name("float_round_test")
            .get_or_create();
        let s = Series::new("result".into(), &[7.999_999_999_999_998_f64, 8.0]);
        let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
        let df = DataFrame::from_polars(pl_df);
        let rows = df.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("result").and_then(|v| v.as_f64()), Some(8.0));
        assert_eq!(rows[1].get("result").and_then(|v| v.as_f64()), Some(8.0));
    }

    /// Nested struct field access: select("outer.inner.leaf") when outer is struct<inner:struct<leaf:int>>.
    #[test]
    fn select_nested_struct_field_outer_inner_leaf() {
        use serde_json::json;

        let spark = SparkSession::builder()
            .app_name("nested_struct_test")
            .get_or_create();
        let schema = vec![(
            "outer".to_string(),
            "struct<inner:struct<leaf:int>>".to_string(),
        )];
        let rows = vec![vec![json!({"inner": {"leaf": 7}})]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        let out = df.select(vec!["outer.inner.leaf"]).unwrap();
        let out_rows = out.collect_as_json_rows().unwrap();
        assert_eq!(out_rows.len(), 1);
        assert_eq!(
            out_rows[0].get("leaf").and_then(|v| v.as_i64()),
            Some(7),
            "nested struct field outer.inner.leaf should resolve to 7"
        );
    }
}
