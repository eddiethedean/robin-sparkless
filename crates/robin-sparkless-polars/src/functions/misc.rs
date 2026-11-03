//! Misc expression builders.

use super::imports::*;
use super::numeric::{ceil, degrees, log, pow, radians};
use super::strings::{lower, substring, substring_with, upper};

/// Concatenate columns as strings (PySpark concat). Numeric columns are cast to string (#852).
pub fn concat(columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "concat requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns
        .iter()
        .map(|c| c.expr().clone().cast(DataType::String))
        .collect();
    Ok(crate::column::Column::from_expr(
        concat_str(&exprs, "", false),
        None,
    ))
}

/// Convert one concat_ws argument to Utf8: scalar columns are cast to string; list columns
/// have elements stringified and joined with `separator` (PySpark parity; #1543).
fn expr_for_concat_ws_part(expr: Expr, separator: &str) -> Expr {
    use polars::prelude::*;
    let sep = separator.to_string();
    expr.map(
        move |col| crate::column::expect_col(concat_ws_input_column_to_string(col, &sep)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::String)),
    )
}

fn concat_ws_input_column_to_string(
    col: polars::prelude::Column,
    separator: &str,
) -> PolarsResult<Option<polars::prelude::Column>> {
    let out = concat_ws_input_series_to_string(col.take_materialized_series(), separator)?;
    Ok(Some(out.into_column()))
}

fn concat_ws_input_series_to_string(s: Series, separator: &str) -> PolarsResult<Series> {
    use polars::chunked_array::builder::StringChunkedBuilder;
    use polars::prelude::*;
    if !matches!(s.dtype(), DataType::List(_)) {
        return s.cast(&DataType::String);
    }
    let list = s.list()?;
    let mut builder = StringChunkedBuilder::new(s.name().clone(), list.len());
    for opt_inner in list.into_iter() {
        let joined = match opt_inner {
            None => None,
            Some(inner) => {
                let inner = inner.cast(&DataType::String)?;
                let ca = inner.str()?;
                let mut parts = Vec::new();
                for v in ca.into_iter().flatten() {
                    parts.push(v);
                }
                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(separator))
                }
            }
        };
        builder.append_option(joined.as_deref());
    }
    Ok(builder.finish().into_series())
}

/// Concatenate columns as strings with separator (PySpark concat_ws). Numeric columns cast to string (#852).
/// List/array columns are element-joined with `separator` first; string columns pass through (#1543).
pub fn concat_ws(separator: &str, columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "concat_ws requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns
        .iter()
        .map(|c| expr_for_concat_ws_part(c.expr().clone(), separator))
        .collect();
    // PySpark semantics: concat_ws ignores nulls across all arguments, and returns
    // null only when all inputs are null. Polars' concat_str with ignore_nulls=true
    // implements the same behavior.
    Ok(crate::column::Column::from_expr(
        concat_str(&exprs, separator, true),
        None,
    ))
}

/// Row number window function (1, 2, 3 by order within partition).
/// Use with `.over(partition_by)` after ranking by an order column.
///
/// # Example
/// ```
/// use robin_sparkless_polars::{col, Column};
/// let salary_col = col("salary");
/// let rn = salary_col.row_number(true).over(&["dept"]);
/// ```
pub fn row_number(column: &Column) -> Column {
    column.clone().row_number(false)
}

/// Rank window function (ties same rank, gaps). Use with `.over(partition_by)`.
pub fn rank(column: &Column, descending: bool) -> Column {
    column.clone().rank(descending)
}

/// Dense rank window function (no gaps). Use with `.over(partition_by)`.
pub fn dense_rank(column: &Column, descending: bool) -> Column {
    column.clone().dense_rank(descending)
}

/// Lag: value from n rows before in partition. Use with `.over(partition_by)`.
pub fn lag(column: &Column, n: i64) -> Column {
    column.clone().lag(n)
}

/// Lead: value from n rows after in partition. Use with `.over(partition_by)`.
pub fn lead(column: &Column, n: i64) -> Column {
    column.clone().lead(n)
}

/// First value in partition (PySpark first_value). Use with `.over(partition_by)`.
pub fn first_value(column: &Column) -> Column {
    column.clone().first_value()
}

/// Last value in partition (PySpark last_value). Use with `.over(partition_by)`.
pub fn last_value(column: &Column) -> Column {
    column.clone().last_value()
}

/// Percent rank in partition: (rank - 1) / (count - 1). Window is applied.
pub fn percent_rank(column: &Column, partition_by: &[&str], descending: bool) -> Column {
    column.clone().percent_rank(partition_by, descending)
}

/// Cumulative distribution in partition: row_number / count. Window is applied.
pub fn cume_dist(column: &Column, partition_by: &[&str], descending: bool) -> Column {
    column.clone().cume_dist(partition_by, descending)
}

/// Ntile: bucket 1..n by rank within partition. Window is applied.
pub fn ntile(column: &Column, n: u32, partition_by: &[&str], descending: bool) -> Column {
    column.clone().ntile(n, partition_by, descending)
}

/// Nth value in partition by order (1-based n). Window is applied; do not call .over() again.
pub fn nth_value(column: &Column, n: i64, partition_by: &[&str], descending: bool) -> Column {
    column.clone().nth_value(n, partition_by, descending)
}

/// Coalesce - returns the first non-null value from multiple columns.
///
/// # Example
/// ```
/// use robin_sparkless_polars::functions::{col, lit_i64, coalesce};
///
/// // coalesce(col("a"), col("b"), lit(0))
/// let expr = coalesce(&[&col("a"), &col("b"), &lit_i64(0)]);
/// ```
/// First non-null value across columns (PySpark coalesce).
pub fn coalesce(columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "coalesce requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let expr = coalesce(&exprs);
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Alias for coalesce(col, value). PySpark nvl / ifnull.
pub fn nvl(column: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let cond = column.expr().clone().is_null();
    let expr = when(cond)
        .then(value.expr().clone())
        .otherwise(column.expr().clone());
    crate::column::Column::from_expr(expr, None)
}

/// Alias for nvl. PySpark ifnull.
pub fn ifnull(column: &Column, value: &Column) -> Column {
    nvl(column, value)
}

/// Return null if column equals value, else column. PySpark nullif.
pub fn nullif(column: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let cond = column.expr().clone().eq(value.expr().clone());
    let null_lit = lit(NULL);
    let expr = when(cond).then(null_lit).otherwise(column.expr().clone());
    crate::column::Column::from_expr(expr, None)
}

/// Replace NaN with value. PySpark nanvl.
pub fn nanvl(column: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let cond = column.expr().clone().is_nan();
    let expr = when(cond)
        .then(value.expr().clone())
        .otherwise(column.expr().clone());
    crate::column::Column::from_expr(expr, None)
}

/// Three-arg null replacement: if col1 is not null then col2 else col3. PySpark nvl2.
pub fn nvl2(col1: &Column, col2: &Column, col3: &Column) -> Column {
    use polars::prelude::*;
    let cond = col1.expr().clone().is_not_null();
    let expr = when(cond)
        .then(col2.expr().clone())
        .otherwise(col3.expr().clone());
    crate::column::Column::from_expr(expr, None)
}

/// Alias for substring. PySpark substr.
pub fn substr(column: &Column, start: i64, length: Option<i64>) -> Column {
    substring(column, start, length)
}

/// Alias for substring_with. PySpark substr with Column start/length (#1649).
pub fn substr_with(column: &Column, start: &Column, length: Option<&Column>) -> Column {
    substring_with(column, start, length)
}

/// Alias for pow. PySpark power.
pub fn power(column: &Column, exp: i64) -> Column {
    pow(column, exp)
}

/// Alias for log (natural log). PySpark ln.
pub fn ln(column: &Column) -> Column {
    log(column)
}

/// Alias for ceil. PySpark ceiling.
pub fn ceiling(column: &Column) -> Column {
    ceil(column)
}

/// Alias for lower. PySpark lcase.
pub fn lcase(column: &Column) -> Column {
    lower(column)
}

/// Alias for upper. PySpark ucase.
pub fn ucase(column: &Column) -> Column {
    upper(column)
}

/// Alias for day. PySpark dayofmonth.
pub fn dayofmonth(column: &Column) -> Column {
    column.clone().dayofmonth()
}

/// Alias for degrees. PySpark toDegrees.
pub fn to_degrees(column: &Column) -> Column {
    degrees(column)
}

/// Alias for radians. PySpark toRadians.
pub fn to_radians(column: &Column) -> Column {
    radians(column)
}

/// Hyperbolic cosine (PySpark cosh).
pub fn cosh(column: &Column) -> Column {
    column.clone().cosh()
}
/// Hyperbolic sine (PySpark sinh).
pub fn sinh(column: &Column) -> Column {
    column.clone().sinh()
}
/// Hyperbolic tangent (PySpark tanh).
pub fn tanh(column: &Column) -> Column {
    column.clone().tanh()
}
/// Inverse hyperbolic cosine (PySpark acosh).
pub fn acosh(column: &Column) -> Column {
    column.clone().acosh()
}
/// Inverse hyperbolic sine (PySpark asinh).
pub fn asinh(column: &Column) -> Column {
    column.clone().asinh()
}
/// Inverse hyperbolic tangent (PySpark atanh).
pub fn atanh(column: &Column) -> Column {
    column.clone().atanh()
}
/// Cube root (PySpark cbrt).
pub fn cbrt(column: &Column) -> Column {
    column.clone().cbrt()
}
/// exp(x) - 1 (PySpark expm1).
pub fn expm1(column: &Column) -> Column {
    column.clone().expm1()
}
/// log(1 + x) (PySpark log1p).
pub fn log1p(column: &Column) -> Column {
    column.clone().log1p()
}
/// Base-10 log (PySpark log10).
pub fn log10(column: &Column) -> Column {
    column.clone().log10()
}
/// Base-2 log (PySpark log2).
pub fn log2(column: &Column) -> Column {
    column.clone().log2()
}
/// Round to nearest integer (PySpark rint).
pub fn rint(column: &Column) -> Column {
    column.clone().rint()
}
/// sqrt(x*x + y*y) (PySpark hypot).
pub fn hypot(x: &Column, y: &Column) -> Column {
    let xx = x.expr().clone() * x.expr().clone();
    let yy = y.expr().clone() * y.expr().clone();
    crate::column::Column::from_expr((xx + yy).sqrt(), None)
}

/// True if column is null. PySpark isnull.
pub fn isnull(column: &Column) -> Column {
    column.clone().is_null()
}

/// True if column is not null. PySpark isnotnull.
pub fn isnotnull(column: &Column) -> Column {
    column.clone().is_not_null()
}
