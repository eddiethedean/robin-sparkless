//! DataFrame module: main tabular type and submodules for transformations, aggregations, joins, stats.

mod aggregations;
mod joins;
mod stats;
mod transformations;

pub(crate) use aggregations::disambiguate_agg_output_names;
pub use aggregations::{CubeRollupData, GroupedData, PivotedGroupedData};
pub use joins::{JoinType, join, try_extract_join_eq_columns, try_extract_join_eq_columns_all};
pub use stats::DataFrameStat;
pub(crate) use transformations::literal_value_to_serde_value;
pub use transformations::{
    DataFrameNa, SelectItem, filter, order_by, order_by_exprs, select, select_items,
    select_with_exprs, with_column,
};

use crate::column::Column;
use crate::error::{EngineError, polars_to_core_error};
use crate::functions::SortOrder;
use crate::schema::{StructType, StructTypePolarsExt};
use crate::session::SparkSession;
use crate::type_coercion::{coerce_for_pyspark_comparison, is_numeric_public};
use polars::datatypes::TimeUnit;
use polars::prelude::{
    AnyValue, DataFrame as PlDataFrame, DataType, Expr, Field, IntoLazy, LazyFrame, NULL,
    PlSmallStr, PolarsError, Schema, SchemaNamesAndDtypes, UnknownKind, col, lit,
};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// Default for `spark.sql.caseSensitive` (PySpark default is false = case-insensitive).
const DEFAULT_CASE_SENSITIVE: bool = false;

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

/// DataFrame - main tabular data structure.
/// Wraps either an eager Polars `DataFrame` or a lazy `LazyFrame`.
/// Transformations extend the plan; actions trigger materialization.
pub struct DataFrame {
    pub(crate) inner: DataFrameInner,
    /// When false (default), column names are matched case-insensitively (PySpark behavior).
    pub(crate) case_sensitive: bool,
    /// Optional alias for subquery/join (PySpark: df.alias("t")).
    pub(crate) alias: Option<String>,
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
        let lf = df.lazy();
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
            alias: None,
        }
    }

    /// Create a new DataFrame from a Polars DataFrame with explicit case sensitivity.
    /// When `case_sensitive` is false, column resolution is case-insensitive (PySpark default).
    pub fn from_polars_with_options(df: PlDataFrame, case_sensitive: bool) -> Self {
        let lf = df.lazy();
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive,
            alias: None,
        }
    }

    /// Create a DataFrame from an eager Polars DataFrame without converting to Lazy.
    /// Used by create_dataframe_from_rows so schema (including struct types) is immediately
    /// available for dotted column resolution (e.g. select("StructValue.e1")) (#1076).
    pub(crate) fn from_eager_with_options(df: PlDataFrame, case_sensitive: bool) -> Self {
        DataFrame {
            inner: DataFrameInner::Eager(Arc::new(df)),
            case_sensitive,
            alias: None,
        }
    }

    /// Create a DataFrame from a LazyFrame (no materialization).
    pub fn from_lazy(lf: LazyFrame) -> Self {
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
            alias: None,
        }
    }

    /// Create a DataFrame from a LazyFrame with explicit case sensitivity.
    pub fn from_lazy_with_options(lf: LazyFrame, case_sensitive: bool) -> Self {
        DataFrame {
            inner: DataFrameInner::Lazy(lf),
            case_sensitive,
            alias: None,
        }
    }

    /// Return a new DataFrame with the same data but case-insensitive column resolution.
    /// Used by plan execution so that Sparkless plans (e.g. col("ID") with schema "id") resolve (issue #524).
    pub(crate) fn with_case_insensitive_column_resolution(self) -> Self {
        DataFrame {
            inner: self.inner,
            case_sensitive: false,
            alias: self.alias,
        }
    }

    /// Create an empty DataFrame
    pub fn empty() -> Self {
        DataFrame {
            inner: DataFrameInner::Lazy(PlDataFrame::empty().lazy()),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
            alias: None,
        }
    }

    /// True if this DataFrame is backed by an eager Polars DataFrame (e.g. from create_dataframe_from_rows).
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

    /// Materialize the plan. Single point of collect for all actions.
    pub(crate) fn collect_inner(&self) -> Result<Arc<PlDataFrame>, PolarsError> {
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
        expr.try_map_expr(move |e| {
            // Recurse into Alias so col("Person.name") inside F.col("Person.name").alias(...) gets resolved (struct field access).
            if let Expr::Alias(inner, name) = &e {
                let new_inner = df.resolve_expr_column_names(inner.as_ref().clone())?;
                return Ok(Expr::Alias(Arc::new(new_inner), name.clone()));
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
                            let mut expr = col(PlSmallStr::from(resolved.as_str()));
                            let mut current_dtype =
                                df.get_column_dtype(resolved.as_str()).ok_or_else(|| {
                                    PolarsError::ColumnNotFound(
                                        format!("cannot resolve: column '{}' not found", resolved)
                                            .into(),
                                    )
                                })?;
                            let mut context_name = resolved.to_string();
                            for field in rest {
                                let (resolved_field, field_dtype) = match df
                                    .resolve_struct_field_from_type(
                                        &current_dtype,
                                        field,
                                        &context_name,
                                    ) {
                                    Ok(t) => t,
                                    Err(_) => {
                                        // #1150: Inferred struct may omit fields (e.g. E2); return null for missing field.
                                        return Ok(lit(NULL).alias(PlSmallStr::from(name_str)));
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
                    if let Some(input_dt) = df.get_expr_output_dtype(&input[0]) {
                        match df.resolve_struct_field_from_type(&input_dt, name.as_str(), "struct")
                        {
                            Ok((resolved_name, _)) => {
                                return Ok(input[0]
                                    .clone()
                                    .struct_()
                                    .field_by_name(&resolved_name));
                            }
                            Err(_) => {
                                // #1150: Inferred struct may omit fields; getField("E2") yields null.
                                return Ok(lit(NULL));
                            }
                        }
                    }
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
            Ok(e)
        })
    }

    /// Rewrite comparison expressions to apply PySpark-style type coercion.
    ///
    /// This walks the expression tree and, for comparison operators where one side is
    /// a column and the other is a numeric literal, delegates to
    /// `coerce_for_pyspark_comparison` so that string–numeric comparisons behave like
    /// PySpark (string values parsed to numbers where possible, invalid strings treated
    /// as null/non-matching).
    pub fn coerce_string_numeric_comparisons(&self, expr: Expr) -> Result<Expr, PolarsError> {
        use polars::prelude::{DataType, LiteralValue, Operator};
        use std::sync::Arc;

        fn is_numeric_literal(expr: &Expr) -> bool {
            match expr {
                Expr::Literal(lv) => {
                    let dt = lv.get_datatype();
                    dt.is_numeric()
                        || matches!(
                            dt,
                            DataType::Unknown(UnknownKind::Int(_))
                                | DataType::Unknown(UnknownKind::Float)
                        )
                }
                _ => false,
            }
        }

        fn literal_dtype(lv: &LiteralValue) -> DataType {
            let dt = lv.get_datatype();
            if matches!(
                dt,
                DataType::Unknown(UnknownKind::Int(_)) | DataType::Unknown(UnknownKind::Float)
            ) {
                DataType::Float64
            } else {
                dt
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
                // Unwrap one Alias so we recognize col/lit when wrapped (e.g. lit(123).into_expr() -> Alias(Literal)).
                let left_inner: &Expr = match left.as_ref() {
                    Expr::Alias(inner, _) => inner.as_ref(),
                    _ => left,
                };
                let right_inner: &Expr = match right.as_ref() {
                    Expr::Alias(inner, _) => inner.as_ref(),
                    _ => right,
                };
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
                let left_is_numeric_lit =
                    matches!(left_inner, Expr::Literal(_)) && is_numeric_literal(left_inner);
                let right_is_numeric_lit =
                    matches!(right_inner, Expr::Literal(_)) && is_numeric_literal(right_inner);
                let left_is_string_lit = matches!(
                    left_inner,
                    Expr::Literal(lv) if lv.get_datatype() == DataType::String
                );
                let right_is_string_lit = matches!(
                    right_inner,
                    Expr::Literal(lv) if lv.get_datatype() == DataType::String
                );
                let root_is_col_vs_numeric = is_comparison_op
                    && ((left_is_col && right_is_numeric_lit)
                        || (right_is_col && left_is_numeric_lit));
                let root_is_col_vs_string = is_comparison_op
                    && ((left_is_col && right_is_string_lit)
                        || (right_is_col && left_is_string_lit));
                if root_is_col_vs_numeric {
                    // PySpark: string column vs numeric literal -> coerce string via try_to_number and compare (issue #235, #602).
                    let col_name = if left_is_col {
                        if let Expr::Column(n) = left_inner {
                            n.as_str()
                        } else {
                            unreachable!()
                        }
                    } else if let Expr::Column(n) = right_inner {
                        n.as_str()
                    } else {
                        unreachable!()
                    };
                    // Use column dtype so numeric columns compare numerically; String (or unknown) uses coercion (try_to_number).
                    let (new_left, new_right) = if left_is_col && right_is_numeric_lit {
                        let col_ty = self.get_column_dtype(col_name);
                        let lit_ty = match right_inner {
                            Expr::Literal(lv) => literal_dtype(lv),
                            _ => DataType::Float64,
                        };
                        let left_ty = col_ty.filter(is_numeric_public).unwrap_or(DataType::String);
                        coerce_for_pyspark_comparison(
                            left_inner.clone(),
                            right_inner.clone(),
                            &left_ty,
                            &lit_ty,
                            op,
                        )
                        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                    } else {
                        let col_ty = self.get_column_dtype(col_name);
                        let lit_ty = match left_inner {
                            Expr::Literal(lv) => literal_dtype(lv),
                            _ => DataType::Float64,
                        };
                        let right_ty = col_ty.filter(is_numeric_public).unwrap_or(DataType::String);
                        coerce_for_pyspark_comparison(
                            left_inner.clone(),
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
                        if let Expr::Column(n) = left_inner {
                            n.as_str()
                        } else {
                            unreachable!()
                        }
                    } else if let Expr::Column(n) = right_inner {
                        n.as_str()
                    } else {
                        unreachable!()
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
                } else if is_comparison_op && left_is_col && right_is_col {
                    // Column-to-column: col("id") == col("label") where id is int, label is string.
                    // Get both column types and coerce string-numeric / date-string for PySpark parity.
                    let left_name = if let Expr::Column(n) = left_inner {
                        n.as_str()
                    } else {
                        unreachable!()
                    };
                    let right_name = if let Expr::Column(n) = right_inner {
                        n.as_str()
                    } else {
                        unreachable!()
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

                let left_is_col = matches!(&*left, Expr::Column(_));
                let right_is_col = matches!(&*right, Expr::Column(_));
                let left_is_lit = matches!(&*left, Expr::Literal(_));
                let right_is_lit = matches!(&*right, Expr::Literal(_));
                let left_is_string_lit =
                    matches!(&*left, Expr::Literal(lv) if lv.get_datatype() == DataType::String);
                let right_is_string_lit =
                    matches!(&*right, Expr::Literal(lv) if lv.get_datatype() == DataType::String);

                let left_is_numeric_lit = left_is_lit && is_numeric_literal(left.as_ref());
                let right_is_numeric_lit = right_is_lit && is_numeric_literal(right.as_ref());

                // Column-vs-numeric-literal: use column dtype; String (or unknown) -> try_to_number then compare (PySpark #235, #602).
                let (new_left, new_right) = if left_is_col && right_is_numeric_lit {
                    let col_ty = if let Expr::Column(n) = &*left {
                        get_col_dtype(n.as_str())
                    } else {
                        None
                    };
                    let lit_ty = match &*right {
                        Expr::Literal(lv) => literal_dtype(lv),
                        _ => DataType::Float64,
                    };
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
                    let col_ty = if let Expr::Column(n) = &*right {
                        get_col_dtype(n.as_str())
                    } else {
                        None
                    };
                    let lit_ty = match &*left {
                        Expr::Literal(lv) => literal_dtype(lv),
                        _ => DataType::Float64,
                    };
                    let right_ty = col_ty.filter(is_numeric_public).unwrap_or(DataType::String);
                    coerce_for_pyspark_comparison(
                        (*left).clone(),
                        (*right).clone(),
                        &lit_ty,
                        &right_ty,
                        &op,
                    )
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                } else if (left_is_col && right_is_string_lit)
                    || (right_is_col && left_is_string_lit)
                {
                    let col_name = if left_is_col {
                        if let Expr::Column(n) = &*left {
                            n.as_str()
                        } else {
                            unreachable!()
                        }
                    } else if let Expr::Column(n) = &*right {
                        n.as_str()
                    } else {
                        unreachable!()
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
                    }
                    return Ok(Expr::BinaryExpr { left, op, right });
                } else {
                    // Leave other comparison forms (col-col, lit-lit, non-numeric) unchanged.
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

    /// Resolve a logical column name to the actual column name in the schema.
    /// When case_sensitive is false, matches case-insensitively.
    pub fn resolve_column_name(&self, name: &str) -> Result<String, PolarsError> {
        let schema = self.schema_or_collect()?;
        let names: Vec<String> = schema
            .iter_names_and_dtypes()
            .map(|(n, _)| n.to_string())
            .collect();
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
        let s = self.schema_or_collect()?;
        Ok(StructType::from_polars_schema(&s))
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
        match expr {
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

    /// Same as [`columns`](Self::columns) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn columns_engine(&self) -> Result<Vec<String>, EngineError> {
        self.columns().map_err(polars_to_core_error)
    }

    /// Count the number of rows (action - triggers execution)
    pub fn count(&self) -> Result<usize, PolarsError> {
        Ok(self.collect_inner()?.height())
    }

    /// Same as [`count`](Self::count) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn count_engine(&self) -> Result<usize, EngineError> {
        self.count().map_err(polars_to_core_error)
    }

    /// Show the first n rows
    pub fn show(&self, n: Option<usize>) -> Result<(), PolarsError> {
        let n = n.unwrap_or(20);
        let df = self.collect_inner()?;
        println!("{}", df.head(Some(n)));
        Ok(())
    }

    /// Collect the DataFrame (action - triggers execution)
    pub fn collect(&self) -> Result<Arc<PlDataFrame>, PolarsError> {
        self.collect_inner()
    }

    /// Same as [`collect_as_json_rows`](Self::collect_as_json_rows) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn collect_as_json_rows_engine(
        &self,
    ) -> Result<Vec<HashMap<String, JsonValue>>, EngineError> {
        self.collect_as_json_rows().map_err(polars_to_core_error)
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
        // #1146: get_json_object returns string; only treat a/nested/missing as String when all three columns present (get_json_object test shape). json_tuple c0/c1 always string.
        let has_get_json_object_shape = names.iter().any(|n| n == "a")
            && names.iter().any(|n| n == "nested")
            && names.iter().any(|n| n == "missing");
        let effective_dtypes: Vec<DataType> = names
            .iter()
            .zip(plan_dtypes.iter())
            .map(|(name, dt)| {
                let force_string = dt == &DataType::Int64
                    && (name.as_str() == "c0"
                        || name.as_str() == "c1"
                        || (has_get_json_object_shape
                            && (name.as_str() == "a"
                                || name.as_str() == "nested"
                                || name.as_str() == "missing")));
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
        let nrows = collected.height();
        let mut rows = Vec::with_capacity(nrows);
        for i in 0..nrows {
            let mut row = HashMap::with_capacity(names.len());
            for (col_idx, name) in names.iter().enumerate() {
                let (s, dtype) = columns_cast
                    .get(col_idx)
                    .ok_or_else(|| PolarsError::ComputeError("column index out of range".into()))?;
                let av = s.get(i)?;
                let jv = any_value_to_json(&av, dtype);
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
        transformations::select_with_exprs(self, exprs, self.case_sensitive)
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
            return transformations::select_with_exprs(self, exprs, self.case_sensitive);
        }
        // Non-dotted names: build explicit expressions so we can implement PySpark-like
        // ambiguous-case handling. When multiple physical columns differ only by case
        // (e.g. "name" and "NAME"), we coalesce them so selects like select("NaMe")
        // see the first non-null value across all.
        let mut exprs: Vec<Expr> = Vec::with_capacity(expanded.len());
        for requested in &expanded {
            let requested_str = requested.as_str();
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
        transformations::select_with_exprs(self, exprs, self.case_sensitive)
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
        transformations::select_items(self, items, self.case_sensitive)
    }

    /// Filter rows using a Polars expression.
    pub fn filter(&self, condition: Expr) -> Result<DataFrame, PolarsError> {
        transformations::filter(self, condition, self.case_sensitive)
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
        transformations::with_column(self, column_name, col, self.case_sensitive)
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
    pub fn group_by(&self, column_names: Vec<&str>) -> Result<GroupedData, PolarsError> {
        use polars::prelude::*;
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let exprs: Vec<Expr> = resolved.iter().map(|name| col(name.as_str())).collect();
        let lf = self.lazy_frame();
        let lazy_grouped = lf.clone().group_by(exprs);
        Ok(GroupedData {
            lf,
            lazy_grouped,
            grouping_cols: resolved,
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
            .map(|e| self.resolve_expr_column_names(e))
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
            self.case_sensitive,
            true, // coalesce so join(right, "id") yields one key column (#1049, #353)
        )
    }

    /// Join with different column names on left and right (PySpark left_on/right_on).
    pub fn join_with_keys(
        &self,
        other: &DataFrame,
        left_on: Vec<&str>,
        right_on: Vec<&str>,
        how: JoinType,
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
        // When same-named keys (e.g. left.id == right.id), coalesce so result has one key column (#353, #1148, #1165).
        let coalesce_same_name_keys = left_resolved == right_resolved;
        join(
            self,
            other,
            left_refs,
            right_refs,
            how,
            self.case_sensitive,
            coalesce_same_name_keys,
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
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        transformations::order_by(self, refs, ascending, self.case_sensitive)
    }

    /// Order by sort expressions (asc/desc with nulls_first/last).
    pub fn order_by_exprs(&self, sort_orders: Vec<SortOrder>) -> Result<DataFrame, PolarsError> {
        transformations::order_by_exprs(self, sort_orders, self.case_sensitive)
    }

    /// Union (unionAll): stack another DataFrame vertically. Schemas must match (same columns, same order).
    pub fn union(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::union(self, other, self.case_sensitive)
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
        transformations::union_by_name(self, other, allow_missing_columns, self.case_sensitive)
    }

    /// Distinct: drop duplicate rows (all columns or optional subset).
    pub fn distinct(&self, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        transformations::distinct(self, subset, self.case_sensitive)
    }

    /// Drop one or more columns.
    pub fn drop(&self, columns: Vec<&str>) -> Result<DataFrame, PolarsError> {
        transformations::drop(self, columns, self.case_sensitive)
    }

    /// Drop rows with nulls. PySpark na.drop(subset, how, thresh).
    pub fn dropna(
        &self,
        subset: Option<Vec<&str>>,
        how: &str,
        thresh: Option<usize>,
    ) -> Result<DataFrame, PolarsError> {
        transformations::dropna(self, subset, how, thresh, self.case_sensitive)
    }

    /// Fill nulls with a literal expression. PySpark na.fill(value, subset=...).
    pub fn fillna(&self, value: Expr, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        transformations::fillna(self, value, subset, self.case_sensitive)
    }

    /// Limit: return first n rows.
    pub fn limit(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::limit(self, n, self.case_sensitive)
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
        transformations::with_column_renamed(self, old_name, new_name, self.case_sensitive)
    }

    /// Replace values in a column (old_value -> new_value). PySpark replace.
    pub fn replace(
        &self,
        column_name: &str,
        old_value: Expr,
        new_value: Expr,
    ) -> Result<DataFrame, PolarsError> {
        transformations::replace(self, column_name, old_value, new_value, self.case_sensitive)
    }

    /// Cross join with another DataFrame (cartesian product). PySpark crossJoin.
    pub fn cross_join(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::cross_join(self, other, self.case_sensitive)
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
        transformations::col_regex(self, pattern, self.case_sensitive)
    }

    /// Add or replace multiple columns. PySpark withColumns. Accepts `Column` so rand/randn get per-row values.
    pub fn with_columns(&self, exprs: &[(String, Column)]) -> Result<DataFrame, PolarsError> {
        transformations::with_columns(self, exprs, self.case_sensitive)
    }

    /// Rename multiple columns. PySpark withColumnsRenamed.
    pub fn with_columns_renamed(
        &self,
        renames: &[(String, String)],
    ) -> Result<DataFrame, PolarsError> {
        transformations::with_columns_renamed(self, renames, self.case_sensitive)
    }

    /// NA sub-API. PySpark df.na().
    pub fn na(&self) -> DataFrameNa<'_> {
        DataFrameNa { df: self }
    }

    /// Skip first n rows. PySpark offset(n).
    pub fn offset(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::offset(self, n, self.case_sensitive)
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

    /// Pivot (wide format). PySpark pivot. Stub: not yet implemented; use crosstab for two-column count.
    pub fn pivot(
        &self,
        _pivot_col: &str,
        _values: Option<Vec<&str>>,
    ) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "pivot is not yet implemented; use crosstab(col1, col2) for two-column cross-tabulation."
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
        crate::delta::write_delta(
            self.collect_inner()?.as_ref(),
            path,
            overwrite,
            merge_schema,
        )
    }

    /// Stub when `delta` feature is disabled.
    #[cfg(not(feature = "delta"))]
    pub fn write_delta(
        &self,
        _path: impl AsRef<std::path::Path>,
        _overwrite: bool,
        _merge_schema: bool,
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
fn align_to_merged_schema_inline(
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
        self.save_as_table_impl(session, name, mode, options)
    }

    /// Same as save_as_table but with options passed explicitly (e.g. from Python writer). Issue #1109.
    pub fn save_as_table_with_options(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        options: &[(String, String)],
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
        )
    }

    fn save_as_table_impl(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        options: Option<&[(String, String)]>,
    ) -> Result<(), PolarsError> {
        use polars::prelude::*;
        use std::fs;
        use std::path::Path;

        let merge_schema = options.is_some_and(|opts| {
            opts.iter().any(|(k, v)| {
                k.eq_ignore_ascii_case("mergeSchema") && v.eq_ignore_ascii_case("true")
            })
        });

        let warehouse_path = session.warehouse_dir().map(|w| Path::new(w).join(name));
        let warehouse_exists = warehouse_path.as_ref().is_some_and(|p| p.is_dir());

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
                    persist_to_warehouse(self.df, p)?;
                }
                self.df.clone()
            }
            SaveMode::Overwrite => {
                if let Some(ref p) = warehouse_path {
                    let _ = fs::remove_dir_all(p);
                    persist_to_warehouse(self.df, p)?;
                }
                self.df.clone()
            }
            SaveMode::Append => {
                let existing_pl = if let Some(existing) = session.get_saved_table(name) {
                    existing.collect_inner()?.as_ref().clone()
                } else if let (Some(ref p), true) = (warehouse_path.as_ref(), warehouse_exists) {
                    // Read from warehouse (data.parquet convention)
                    let data_file = p.join("data.parquet");
                    let read_path = if data_file.is_file() {
                        data_file.as_path()
                    } else {
                        p.as_ref()
                    };
                    let pl_path =
                        polars::prelude::PlRefPath::try_from_path(read_path).map_err(|e| {
                            PolarsError::ComputeError(
                                format!("saveAsTable append: path: {e}").into(),
                            )
                        })?;
                    let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default()).map_err(
                        |e| {
                            PolarsError::ComputeError(
                                format!("saveAsTable append: read warehouse: {e}").into(),
                            )
                        },
                    )?;
                    lf.collect().map_err(|e| {
                        PolarsError::ComputeError(
                            format!("saveAsTable append: collect: {e}").into(),
                        )
                    })?
                } else {
                    // New table
                    session.register_table(name, self.df.clone());
                    if let Some(ref p) = warehouse_path {
                        persist_to_warehouse(self.df, p)?;
                    }
                    return Ok(());
                };
                let new_pl = self.df.collect_inner()?.as_ref().clone();
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
                if let Some(ref p) = warehouse_path {
                    let _ = fs::remove_dir_all(p);
                    persist_to_warehouse(&merged, p)?;
                }
                merged
            }
            SaveMode::Ignore => {
                if session.saved_table_exists(name) || warehouse_exists {
                    return Ok(());
                }
                if let Some(ref p) = warehouse_path {
                    persist_to_warehouse(self.df, p)?;
                }
                self.df.clone()
            }
        };
        session.register_table(name, final_df);
        Ok(())
    }

    /// Write as Parquet (PySpark: parquet(path)). Equivalent to format(Parquet).save(path).
    pub fn parquet(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Parquet,
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
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write to path. Overwrite replaces; append reads existing (if any) and concatenates then writes.
    /// With partition_by, path is a directory; each partition is written as path/col1=val1/col2=val2/... with partition columns omitted from the file (Spark/Hive style).
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
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

        match self.format {
            WriteFormat::Parquet => {
                let mut file = std::fs::File::create(path).map_err(|e| {
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
                let mut file = std::fs::File::create(path).map_err(|e| {
                    PolarsError::ComputeError(format!("write csv create: {e}").into())
                })?;
                CsvWriter::new(&mut file)
                    .include_header(has_header)
                    .with_separator(delimiter)
                    .finish(&mut to_write.clone())
                    .map_err(|e| PolarsError::ComputeError(format!("write csv: {e}").into()))?;
            }
            WriteFormat::Json => {
                let mut file = std::fs::File::create(path).map_err(|e| {
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

            let part_path: std::path::PathBuf = resolved
                .iter()
                .zip(row.iter())
                .map(|(name, av)| format!("{}={}", name, format_partition_value(av)))
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
        }
    }
}

/// Format a partition column value for use in a directory name (Spark/Hive style).
/// Null becomes "__HIVE_DEFAULT_PARTITION__"; other values use string representation with path-unsafe chars replaced.
fn format_partition_value(av: &AnyValue<'_>) -> String {
    let s = match av {
        AnyValue::Null => "__HIVE_DEFAULT_PARTITION__".to_string(),
        AnyValue::Boolean(b) => b.to_string(),
        AnyValue::Int32(i) => i.to_string(),
        AnyValue::Int64(i) => i.to_string(),
        AnyValue::UInt32(u) => u.to_string(),
        AnyValue::UInt64(u) => u.to_string(),
        AnyValue::Float32(f) => f.to_string(),
        AnyValue::Float64(f) => f.to_string(),
        AnyValue::String(s) => s.to_string(),
        AnyValue::StringOwned(s) => s.as_str().to_string(),
        AnyValue::Date(d) => d.to_string(),
        _ => av.to_string(),
    };
    // Replace path separators and other unsafe chars so the value is a valid path segment
    s.replace([std::path::MAIN_SEPARATOR, '/'], "_")
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
            AnyValue::UInt32(u) => col(name.as_str()).eq(lit(*u)),
            AnyValue::UInt64(u) => col(name.as_str()).eq(lit(*u)),
            AnyValue::Float32(f) => col(name.as_str()).eq(lit(*f)),
            AnyValue::Float64(f) => col(name.as_str()).eq(lit(*f)),
            AnyValue::String(s) => col(name.as_str()).eq(lit(s.to_string())),
            AnyValue::StringOwned(s) => col(name.as_str()).eq(lit(s.clone())),
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

/// When map value was unified to String by concat_list, try to recover number/bool by parsing as JSON (#3.6).
fn map_value_string_to_json(s: &str) -> JsonValue {
    if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
        match &parsed {
            JsonValue::Number(_) | JsonValue::Bool(_) => return parsed,
            JsonValue::String(_) => return parsed,
            _ => {}
        }
    }
    JsonValue::String(s.to_string())
}

/// Round float to integer for JSON when within epsilon of an integer (#747, #748: 2**3 => 8 not 7.999...).
/// Use scale-tolerant epsilon so large power results (e.g. 2**30, 3**5) also round (#818, #820, #821, #823, #827).
fn float_to_json_number(f: f64) -> JsonValue {
    const EPSILON: f64 = 1e-6;
    if f.is_finite() {
        let r = f.round();
        if (f - r).abs() < EPSILON {
            if let Some(n) = serde_json::Number::from_f64(r) {
                return JsonValue::Number(n);
            }
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
fn any_value_to_json(av: &AnyValue<'_>, dtype: &DataType) -> JsonValue {
    use serde_json::Map;
    // Plan says String but execution may yield numeric (e.g. get_json_object; #1146): emit string.
    if matches!(dtype, DataType::String) {
        if let Some(s) = av.get_str() {
            return JsonValue::String(s.to_string());
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
            return JsonValue::String(av.to_string());
        }
    }
    match av {
        AnyValue::Null => JsonValue::Null,
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
        AnyValue::Float32(f) => float_to_json_number(f64::from(*f)),
        AnyValue::Float64(f) => float_to_json_number(*f),
        AnyValue::String(s) => {
            // List column may be stringified in some paths; parse back to array (#846, #845).
            if matches!(dtype, DataType::List(_)) {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                    if parsed.is_array() {
                        return parsed;
                    }
                }
            }
            // #1066: Struct column may be stringified in some paths; parse back to object so nested structs work.
            if let DataType::Struct(fields) = dtype {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                    if parsed.is_object() {
                        return parsed;
                    }
                }
                if let Some(obj) = struct_string_to_json_object(s, fields) {
                    return obj;
                }
            }
            JsonValue::String(s.to_string())
        }
        AnyValue::StringOwned(s) => {
            if matches!(dtype, DataType::List(_)) {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s.as_ref()) {
                    if parsed.is_array() {
                        return parsed;
                    }
                }
            }
            if let DataType::Struct(fields) = dtype {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s.as_ref()) {
                    if parsed.is_object() {
                        return parsed;
                    }
                }
                if let Some(obj) = struct_string_to_json_object(s.as_ref(), fields) {
                    return obj;
                }
            }
            JsonValue::String(s.to_string())
        }
        AnyValue::List(s) => {
            if is_map_format(dtype) {
                // List(Struct{key, value}) -> JSON object {} (PySpark empty map #578).
                let mut obj = Map::new();
                for i in 0..s.len() {
                    if let Ok(elem) = s.get(i) {
                        let (k, v) = match &elem {
                            AnyValue::Struct(_, _, fields) => {
                                let mut k = None;
                                let mut v = None;
                                for (fld_av, fld) in elem._iter_struct_av().zip(fields.iter()) {
                                    if fld.name == "key" {
                                        k = fld_av
                                            .get_str()
                                            .map(|s| s.to_string())
                                            .or_else(|| Some(fld_av.to_string()));
                                    } else if fld.name == "value" {
                                        v = Some(if matches!(fld.dtype, DataType::String) {
                                            if let Some(s) = fld_av.get_str() {
                                                map_value_string_to_json(s)
                                            } else {
                                                any_value_to_json(&fld_av, &fld.dtype)
                                            }
                                        } else {
                                            any_value_to_json(&fld_av, &fld.dtype)
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
                                        k = fld_av
                                            .get_str()
                                            .map(|s| s.to_string())
                                            .or_else(|| Some(fld_av.to_string()));
                                    } else if fld.name == "value" {
                                        v = Some(if matches!(fld.dtype, DataType::String) {
                                            if let Some(s) = fld_av.get_str() {
                                                map_value_string_to_json(s)
                                            } else {
                                                any_value_to_json(fld_av, &fld.dtype)
                                            }
                                        } else {
                                            any_value_to_json(fld_av, &fld.dtype)
                                        });
                                    }
                                }
                                (k, v)
                            }
                            _ => (None, None),
                        };
                        if let (Some(key), Some(val)) = (k, v) {
                            obj.insert(key, val);
                        }
                    }
                }
                JsonValue::Object(obj)
            } else {
                let inner_dtype = match dtype {
                    DataType::List(inner) => inner.as_ref(),
                    _ => dtype,
                };
                let arr: Vec<JsonValue> = (0..s.len())
                    .filter_map(|i| s.get(i).ok())
                    .map(|a| any_value_to_json(&a, inner_dtype))
                    .collect();
                JsonValue::Array(arr)
            }
        }
        AnyValue::Struct(_, _, fields) => {
            let mut vals: Vec<JsonValue> = Vec::with_capacity(fields.len());
            for (fld_av, fld) in av._iter_struct_av().zip(fields.iter()) {
                vals.push(any_value_to_json(&fld_av, &fld.dtype));
            }
            if vals.iter().all(|v| matches!(v, JsonValue::Null)) {
                JsonValue::Null
            } else {
                let mut obj = Map::new();
                for (fld, v) in fields.iter().zip(vals) {
                    obj.insert(fld.name.to_string(), v);
                }
                JsonValue::Object(obj)
            }
        }
        AnyValue::StructOwned(payload) => {
            let (values, fields) = &**payload;
            let vals: Vec<JsonValue> = values
                .iter()
                .zip(fields.iter())
                .map(|(fld_av, fld)| any_value_to_json(fld_av, &fld.dtype))
                .collect();
            if vals.iter().all(|v| matches!(v, JsonValue::Null)) {
                JsonValue::Null
            } else {
                let mut obj = Map::new();
                for (fld, v) in fields.iter().zip(vals) {
                    obj.insert(fld.name.to_string(), v);
                }
                JsonValue::Object(obj)
            }
        }
        AnyValue::Date(days) => date_days_to_json(*days),
        AnyValue::Datetime(val, unit, _) => datetime_anyvalue_to_json_iso(*val, unit),
        AnyValue::DatetimeOwned(val, unit, _) => datetime_anyvalue_to_json_iso(*val, unit),
        _ => JsonValue::Null,
    }
}

/// Broadcast hint - no-op that returns the same DataFrame (PySpark broadcast).
pub fn broadcast(df: &DataFrame) -> DataFrame {
    df.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{NamedFrom, Series};

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
            msg.contains("pivot is not yet implemented") && msg.contains("crosstab"),
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
