use crate::column::Column;
use polars::prelude::*;

/// Get a column by name
pub fn col(name: &str) -> Column {
    Column::new(name.to_string())
}

/// Create a literal column from a value
pub fn lit_i32(value: i32) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

pub fn lit_i64(value: i64) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

pub fn lit_f64(value: f64) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

pub fn lit_bool(value: bool) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

pub fn lit_str(value: &str) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

/// Count aggregation
pub fn count(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().count(), Some("count".to_string()))
}

/// Sum aggregation
pub fn sum(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().sum(), Some("sum".to_string()))
}

/// Average aggregation
pub fn avg(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().mean(), Some("avg".to_string()))
}

/// Maximum aggregation
pub fn max(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().max(), Some("max".to_string()))
}

/// Minimum aggregation
pub fn min(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().min(), Some("min".to_string()))
}

/// Standard deviation (sample) aggregation (PySpark stddev / stddev_samp)
pub fn stddev(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().std(1), Some("stddev".to_string()))
}

/// Variance (sample) aggregation (PySpark variance / var_samp)
pub fn variance(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().var(1), Some("variance".to_string()))
}

/// Count distinct aggregation (PySpark countDistinct)
pub fn count_distinct(col: &Column) -> Column {
    use polars::prelude::DataType;
    Column::from_expr(
        col.expr().clone().n_unique().cast(DataType::Int64),
        Some("count_distinct".to_string()),
    )
}

/// PySpark-style conditional expression builder.
///
/// # Example
/// ```
/// use robin_sparkless::{col, lit_i64, lit_str, when};
///
/// // when(condition).then(value).otherwise(fallback)
/// let expr = when(&col("age").gt(lit_i64(18).into_expr()))
///     .then(&lit_str("adult"))
///     .otherwise(&lit_str("minor"));
/// ```
pub fn when(condition: &Column) -> WhenBuilder {
    WhenBuilder::new(condition.expr().clone())
}

/// Builder for when-then-otherwise expressions
pub struct WhenBuilder {
    condition: Expr,
}

impl WhenBuilder {
    fn new(condition: Expr) -> Self {
        WhenBuilder { condition }
    }

    /// Specify the value when condition is true
    pub fn then(self, value: &Column) -> ThenBuilder {
        use polars::prelude::*;
        let when_then = when(self.condition).then(value.expr().clone());
        ThenBuilder::new(when_then)
    }

    /// Specify the value when condition is false
    /// Note: In PySpark, when(cond).otherwise(val) requires a .then() first.
    /// For this implementation, we require .then() to be called explicitly.
    /// This method will panic if used directly - use when(cond).then(val1).otherwise(val2) instead.
    pub fn otherwise(self, _value: &Column) -> Column {
        // This should not be called directly - when().otherwise() without .then() is not supported
        // Users should use when(cond).then(val1).otherwise(val2)
        panic!("when().otherwise() requires .then() to be called first. Use when(cond).then(val1).otherwise(val2)");
    }
}

/// Builder for chaining when-then clauses before finalizing with otherwise
pub struct ThenBuilder {
    when_then: polars::prelude::Then, // The Polars WhenThen state
}

impl ThenBuilder {
    fn new(when_then: polars::prelude::Then) -> Self {
        ThenBuilder { when_then }
    }

    /// Chain an additional when-then clause
    /// Note: Chaining multiple when-then clauses is not yet fully supported.
    /// For now, use a single when().then().otherwise() pattern.
    pub fn when(self, _condition: &Column) -> ThenBuilder {
        // TODO: Implement proper chaining support
        // For now, return self to allow compilation but chaining won't work correctly
        self
    }

    /// Finalize the expression with the fallback value
    pub fn otherwise(self, value: &Column) -> Column {
        let expr = self.when_then.otherwise(value.expr().clone());
        crate::column::Column::from_expr(expr, None)
    }
}

/// Convert string column to uppercase (PySpark upper)
pub fn upper(column: &Column) -> Column {
    column.clone().upper()
}

/// Convert string column to lowercase (PySpark lower)
pub fn lower(column: &Column) -> Column {
    column.clone().lower()
}

/// Substring with 1-based start (PySpark substring semantics)
pub fn substring(column: &Column, start: i64, length: Option<i64>) -> Column {
    column.clone().substr(start, length)
}

/// String length in characters (PySpark length)
pub fn length(column: &Column) -> Column {
    column.clone().length()
}

/// Trim leading and trailing whitespace (PySpark trim)
pub fn trim(column: &Column) -> Column {
    column.clone().trim()
}

/// Trim leading whitespace (PySpark ltrim)
pub fn ltrim(column: &Column) -> Column {
    column.clone().ltrim()
}

/// Trim trailing whitespace (PySpark rtrim)
pub fn rtrim(column: &Column) -> Column {
    column.clone().rtrim()
}

/// Extract first match of regex (PySpark regexp_extract). group_index 0 = full match.
pub fn regexp_extract(column: &Column, pattern: &str, group_index: usize) -> Column {
    column.clone().regexp_extract(pattern, group_index)
}

/// Replace first match of regex (PySpark regexp_replace)
pub fn regexp_replace(column: &Column, pattern: &str, replacement: &str) -> Column {
    column.clone().regexp_replace(pattern, replacement)
}

/// Split string by delimiter (PySpark split)
pub fn split(column: &Column, delimiter: &str) -> Column {
    column.clone().split(delimiter)
}

/// Title case (PySpark initcap)
pub fn initcap(column: &Column) -> Column {
    column.clone().initcap()
}

/// Absolute value (PySpark abs)
pub fn abs(column: &Column) -> Column {
    column.clone().abs()
}

/// Ceiling (PySpark ceil)
pub fn ceil(column: &Column) -> Column {
    column.clone().ceil()
}

/// Floor (PySpark floor)
pub fn floor(column: &Column) -> Column {
    column.clone().floor()
}

/// Round (PySpark round)
pub fn round(column: &Column, decimals: u32) -> Column {
    column.clone().round(decimals)
}

/// Extract year from datetime column (PySpark year)
pub fn year(column: &Column) -> Column {
    column.clone().year()
}

/// Extract month from datetime column (PySpark month)
pub fn month(column: &Column) -> Column {
    column.clone().month()
}

/// Extract day of month from datetime column (PySpark day)
pub fn day(column: &Column) -> Column {
    column.clone().day()
}

/// Concatenate string columns without separator (PySpark concat)
pub fn concat(columns: &[&Column]) -> Column {
    use polars::prelude::*;
    if columns.is_empty() {
        panic!("concat requires at least one column");
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    crate::column::Column::from_expr(concat_str(&exprs, "", false), None)
}

/// Concatenate string columns with separator (PySpark concat_ws)
pub fn concat_ws(separator: &str, columns: &[&Column]) -> Column {
    use polars::prelude::*;
    if columns.is_empty() {
        panic!("concat_ws requires at least one column");
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    crate::column::Column::from_expr(concat_str(&exprs, separator, false), None)
}

/// Row number window function (1, 2, 3 by order within partition).
/// Use with `.over(partition_by)` after ranking by an order column.
///
/// # Example
/// ```
/// use robin_sparkless::{col, Column};
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

/// Coalesce - returns the first non-null value from multiple columns.
///
/// # Example
/// ```
/// use robin_sparkless::{col, lit_i64, coalesce};
///
/// // coalesce(col("a"), col("b"), lit(0))
/// let expr = coalesce(&[&col("a"), &col("b"), &lit_i64(0)]);
/// ```
pub fn coalesce(columns: &[&Column]) -> Column {
    use polars::prelude::*;
    if columns.is_empty() {
        panic!("coalesce requires at least one column");
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let expr = coalesce(&exprs);
    crate::column::Column::from_expr(expr, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{df, IntoLazy};

    #[test]
    fn test_col_creates_column() {
        let column = col("test");
        assert_eq!(column.name(), "test");
    }

    #[test]
    fn test_lit_i32() {
        let column = lit_i32(42);
        // The column should have a default name since it's a literal
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_lit_i64() {
        let column = lit_i64(123456789012345i64);
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_lit_f64() {
        let column = lit_f64(3.14159);
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_lit_bool() {
        let column = lit_bool(true);
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_lit_str() {
        let column = lit_str("hello");
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_count_aggregation() {
        let column = col("value");
        let result = count(&column);
        assert_eq!(result.name(), "count");
    }

    #[test]
    fn test_sum_aggregation() {
        let column = col("value");
        let result = sum(&column);
        assert_eq!(result.name(), "sum");
    }

    #[test]
    fn test_avg_aggregation() {
        let column = col("value");
        let result = avg(&column);
        assert_eq!(result.name(), "avg");
    }

    #[test]
    fn test_max_aggregation() {
        let column = col("value");
        let result = max(&column);
        assert_eq!(result.name(), "max");
    }

    #[test]
    fn test_min_aggregation() {
        let column = col("value");
        let result = min(&column);
        assert_eq!(result.name(), "min");
    }

    #[test]
    fn test_when_then_otherwise() {
        // Create a simple DataFrame
        let df = df!(
            "age" => &[15, 25, 35]
        )
        .unwrap();

        // Build a when-then-otherwise expression
        let age_col = col("age");
        let condition = age_col.gt(polars::prelude::lit(18));
        let result = when(&condition)
            .then(&lit_str("adult"))
            .otherwise(&lit_str("minor"));

        // Apply the expression
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("status"))
            .collect()
            .unwrap();

        // Verify the result
        let status_col = result_df.column("status").unwrap();
        let values: Vec<Option<&str>> = status_col.str().unwrap().into_iter().collect();

        assert_eq!(values[0], Some("minor")); // age 15 < 18
        assert_eq!(values[1], Some("adult")); // age 25 > 18
        assert_eq!(values[2], Some("adult")); // age 35 > 18
    }

    #[test]
    fn test_coalesce_returns_first_non_null() {
        // Create a DataFrame with some nulls
        let df = df!(
            "a" => &[Some(1), None, None],
            "b" => &[None, Some(2), None],
            "c" => &[None, None, Some(3)]
        )
        .unwrap();

        let col_a = col("a");
        let col_b = col("b");
        let col_c = col("c");
        let result = coalesce(&[&col_a, &col_b, &col_c]);

        // Apply the expression
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("coalesced"))
            .collect()
            .unwrap();

        // Verify the result
        let coalesced_col = result_df.column("coalesced").unwrap();
        let values: Vec<Option<i32>> = coalesced_col.i32().unwrap().into_iter().collect();

        assert_eq!(values[0], Some(1)); // First non-null is 'a'
        assert_eq!(values[1], Some(2)); // First non-null is 'b'
        assert_eq!(values[2], Some(3)); // First non-null is 'c'
    }

    #[test]
    fn test_coalesce_with_literal_fallback() {
        // Create a DataFrame with all nulls in one row
        let df = df!(
            "a" => &[Some(1), None],
            "b" => &[None::<i32>, None::<i32>]
        )
        .unwrap();

        let col_a = col("a");
        let col_b = col("b");
        let fallback = lit_i32(0);
        let result = coalesce(&[&col_a, &col_b, &fallback]);

        // Apply the expression
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("coalesced"))
            .collect()
            .unwrap();

        // Verify the result
        let coalesced_col = result_df.column("coalesced").unwrap();
        let values: Vec<Option<i32>> = coalesced_col.i32().unwrap().into_iter().collect();

        assert_eq!(values[0], Some(1)); // First non-null is 'a'
        assert_eq!(values[1], Some(0)); // All nulls, use fallback
    }

    #[test]
    #[should_panic(expected = "coalesce requires at least one column")]
    fn test_coalesce_empty_panics() {
        let columns: [&Column; 0] = [];
        let _ = coalesce(&columns);
    }
}
