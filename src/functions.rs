use crate::column::Column;
use crate::dataframe::DataFrame;
use polars::prelude::*;

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------

/// Sort order specification for use in orderBy/sort. Holds expr + direction + null placement.
#[derive(Debug, Clone)]
pub struct SortOrder {
    pub(crate) expr: Expr,
    pub(crate) descending: bool,
    pub(crate) nulls_last: bool,
}

impl SortOrder {
    pub fn expr(&self) -> &Expr {
        &self.expr
    }
}

/// Ascending sort, nulls first (Spark default for ASC).
pub fn asc(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: false,
        nulls_last: false,
    }
}

/// Ascending sort, nulls first.
pub fn asc_nulls_first(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: false,
        nulls_last: false,
    }
}

/// Ascending sort, nulls last.
pub fn asc_nulls_last(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: false,
        nulls_last: true,
    }
}

/// Descending sort, nulls last (Spark default for DESC).
pub fn desc(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: true,
        nulls_last: true,
    }
}

/// Descending sort, nulls first.
pub fn desc_nulls_first(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: true,
        nulls_last: false,
    }
}

/// Descending sort, nulls last.
pub fn desc_nulls_last(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: true,
        nulls_last: true,
    }
}

// -----------------------------------------------------------------------------

/// Parse PySpark-like type name to Polars DataType.
pub fn parse_type_name(name: &str) -> Result<DataType, String> {
    let s = name.trim().to_lowercase();
    Ok(match s.as_str() {
        "int" | "integer" => DataType::Int32,
        "long" | "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "string" | "str" => DataType::String,
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date,
        "timestamp" => DataType::Datetime(TimeUnit::Microseconds, None),
        _ => return Err(format!("unknown type name: {name}")),
    })
}

/// Get a column by name
pub fn col(name: &str) -> Column {
    Column::new(name.to_string())
}

/// Grouping set marker (PySpark grouping). Stub: returns 0 (no GROUPING SETS in robin-sparkless).
pub fn grouping(column: &Column) -> Column {
    let _ = column;
    Column::from_expr(lit(0i32), Some("grouping".to_string()))
}

/// Grouping set id (PySpark grouping_id). Stub: returns 0.
pub fn grouping_id(_columns: &[Column]) -> Column {
    Column::from_expr(lit(0i64), Some("grouping_id".to_string()))
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

/// Alias for avg (PySpark mean).
pub fn mean(col: &Column) -> Column {
    avg(col)
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

/// Population standard deviation (ddof=0). PySpark stddev_pop.
pub fn stddev_pop(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().std(0), Some("stddev_pop".to_string()))
}

/// Sample standard deviation (ddof=1). Alias for stddev. PySpark stddev_samp.
pub fn stddev_samp(col: &Column) -> Column {
    stddev(col)
}

/// Alias for stddev (PySpark std).
pub fn std(col: &Column) -> Column {
    stddev(col)
}

/// Population variance (ddof=0). PySpark var_pop.
pub fn var_pop(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().var(0), Some("var_pop".to_string()))
}

/// Sample variance (ddof=1). Alias for variance. PySpark var_samp.
pub fn var_samp(col: &Column) -> Column {
    variance(col)
}

/// Median aggregation. PySpark median.
pub fn median(col: &Column) -> Column {
    use polars::prelude::QuantileMethod;
    Column::from_expr(
        col.expr()
            .clone()
            .quantile(lit(0.5), QuantileMethod::Linear),
        Some("median".to_string()),
    )
}

/// Approximate percentile (PySpark approx_percentile). Maps to quantile; percentage in 0.0..=1.0.
pub fn approx_percentile(col: &Column, percentage: f64) -> Column {
    use polars::prelude::QuantileMethod;
    Column::from_expr(
        col.expr()
            .clone()
            .quantile(lit(percentage), QuantileMethod::Linear),
        Some(format!("approx_percentile({percentage})")),
    )
}

/// Approximate percentile (PySpark percentile_approx). Alias for approx_percentile.
pub fn percentile_approx(col: &Column, percentage: f64) -> Column {
    approx_percentile(col, percentage)
}

/// Mode aggregation - most frequent value. PySpark mode.
pub fn mode(col: &Column) -> Column {
    col.clone().mode()
}

/// Count distinct aggregation (PySpark countDistinct)
pub fn count_distinct(col: &Column) -> Column {
    use polars::prelude::DataType;
    Column::from_expr(
        col.expr().clone().n_unique().cast(DataType::Int64),
        Some("count_distinct".to_string()),
    )
}

/// Kurtosis aggregation (PySpark kurtosis). Fisher definition, bias=true. Use in groupBy.agg().
pub fn kurtosis(col: &Column) -> Column {
    Column::from_expr(
        col.expr()
            .clone()
            .cast(DataType::Float64)
            .kurtosis(true, true),
        Some("kurtosis".to_string()),
    )
}

/// Skewness aggregation (PySpark skewness). bias=true. Use in groupBy.agg().
pub fn skewness(col: &Column) -> Column {
    Column::from_expr(
        col.expr().clone().cast(DataType::Float64).skew(true),
        Some("skewness".to_string()),
    )
}

/// Population covariance aggregation (PySpark covar_pop). Returns Expr for use in groupBy.agg().
pub fn covar_pop_expr(col1: &str, col2: &str) -> Expr {
    use polars::prelude::{col as pl_col, len};
    let c1 = pl_col(col1).cast(DataType::Float64);
    let c2 = pl_col(col2).cast(DataType::Float64);
    let n = len().cast(DataType::Float64);
    let sum_ab = (c1.clone() * c2.clone()).sum();
    let sum_a = pl_col(col1).sum().cast(DataType::Float64);
    let sum_b = pl_col(col2).sum().cast(DataType::Float64);
    (sum_ab - sum_a * sum_b / n.clone()) / n
}

/// Sample covariance aggregation (PySpark covar_samp). Returns Expr for use in groupBy.agg().
pub fn covar_samp_expr(col1: &str, col2: &str) -> Expr {
    use polars::prelude::{col as pl_col, len, lit, when};
    let c1 = pl_col(col1).cast(DataType::Float64);
    let c2 = pl_col(col2).cast(DataType::Float64);
    let n = len().cast(DataType::Float64);
    let sum_ab = (c1.clone() * c2.clone()).sum();
    let sum_a = pl_col(col1).sum().cast(DataType::Float64);
    let sum_b = pl_col(col2).sum().cast(DataType::Float64);
    when(len().gt(lit(1)))
        .then((sum_ab - sum_a * sum_b / n.clone()) / (len() - lit(1)).cast(DataType::Float64))
        .otherwise(lit(f64::NAN))
}

/// Pearson correlation aggregation (PySpark corr). Returns Expr for use in groupBy.agg().
pub fn corr_expr(col1: &str, col2: &str) -> Expr {
    use polars::prelude::{col as pl_col, len, lit, when};
    let c1 = pl_col(col1).cast(DataType::Float64);
    let c2 = pl_col(col2).cast(DataType::Float64);
    let n = len().cast(DataType::Float64);
    let n1 = (len() - lit(1)).cast(DataType::Float64);
    let sum_ab = (c1.clone() * c2.clone()).sum();
    let sum_a = pl_col(col1).sum().cast(DataType::Float64);
    let sum_b = pl_col(col2).sum().cast(DataType::Float64);
    let sum_a2 = (c1.clone() * c1).sum();
    let sum_b2 = (c2.clone() * c2).sum();
    let cov_samp = (sum_ab - sum_a.clone() * sum_b.clone() / n.clone()) / n1.clone();
    let var_a = (sum_a2 - sum_a.clone() * sum_a / n.clone()) / n1.clone();
    let var_b = (sum_b2 - sum_b.clone() * sum_b / n.clone()) / n1.clone();
    let std_a = var_a.sqrt();
    let std_b = var_b.sqrt();
    when(len().gt(lit(1)))
        .then(cov_samp / (std_a * std_b))
        .otherwise(lit(f64::NAN))
}

// --- Regression aggregates (PySpark regr_*). y = col1, x = col2; only pairs where both non-null. ---

fn regr_cond_and_sums(y_col: &str, x_col: &str) -> (Expr, Expr, Expr, Expr, Expr, Expr) {
    use polars::prelude::col as pl_col;
    let y = pl_col(y_col).cast(DataType::Float64);
    let x = pl_col(x_col).cast(DataType::Float64);
    let cond = y.clone().is_not_null().and(x.clone().is_not_null());
    let n = y
        .clone()
        .filter(cond.clone())
        .count()
        .cast(DataType::Float64);
    let sum_x = x.clone().filter(cond.clone()).sum();
    let sum_y = y.clone().filter(cond.clone()).sum();
    let sum_xx = (x.clone() * x.clone()).filter(cond.clone()).sum();
    let sum_yy = (y.clone() * y.clone()).filter(cond.clone()).sum();
    let sum_xy = (x * y).filter(cond).sum();
    (n, sum_x, sum_y, sum_xx, sum_yy, sum_xy)
}

/// Regression: count of (y, x) pairs where both non-null (PySpark regr_count).
pub fn regr_count_expr(y_col: &str, x_col: &str) -> Expr {
    let (n, ..) = regr_cond_and_sums(y_col, x_col);
    n
}

/// Regression: average of x (PySpark regr_avgx).
pub fn regr_avgx_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, ..) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_x / n)
        .otherwise(lit(f64::NAN))
}

/// Regression: average of y (PySpark regr_avgy).
pub fn regr_avgy_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, _, sum_y, ..) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_y / n)
        .otherwise(lit(f64::NAN))
}

/// Regression: sum((x - avg_x)^2) (PySpark regr_sxx).
pub fn regr_sxx_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, _, sum_xx, ..) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_xx - sum_x.clone() * sum_x / n)
        .otherwise(lit(f64::NAN))
}

/// Regression: sum((y - avg_y)^2) (PySpark regr_syy).
pub fn regr_syy_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, _, sum_y, _, sum_yy, _) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_yy - sum_y.clone() * sum_y / n)
        .otherwise(lit(f64::NAN))
}

/// Regression: sum((x - avg_x)(y - avg_y)) (PySpark regr_sxy).
pub fn regr_sxy_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, sum_y, _, _, sum_xy) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_xy - sum_x * sum_y / n)
        .otherwise(lit(f64::NAN))
}

/// Regression slope: cov_samp(y,x)/var_samp(x) (PySpark regr_slope).
pub fn regr_slope_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, sum_y, sum_xx, _sum_yy, sum_xy) = regr_cond_and_sums(y_col, x_col);
    let regr_sxx = sum_xx.clone() - sum_x.clone() * sum_x.clone() / n.clone();
    let regr_sxy = sum_xy - sum_x * sum_y / n.clone();
    when(n.gt(lit(1.0)).and(regr_sxx.clone().gt(lit(0.0))))
        .then(regr_sxy / regr_sxx)
        .otherwise(lit(f64::NAN))
}

/// Regression intercept: avg_y - slope*avg_x (PySpark regr_intercept).
pub fn regr_intercept_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, sum_y, sum_xx, _, sum_xy) = regr_cond_and_sums(y_col, x_col);
    let regr_sxx = sum_xx - sum_x.clone() * sum_x.clone() / n.clone();
    let regr_sxy = sum_xy.clone() - sum_x.clone() * sum_y.clone() / n.clone();
    let slope = regr_sxy.clone() / regr_sxx.clone();
    let avg_y = sum_y / n.clone();
    let avg_x = sum_x / n.clone();
    when(n.gt(lit(1.0)).and(regr_sxx.clone().gt(lit(0.0))))
        .then(avg_y - slope * avg_x)
        .otherwise(lit(f64::NAN))
}

/// Regression R-squared (PySpark regr_r2).
pub fn regr_r2_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, sum_y, sum_xx, sum_yy, sum_xy) = regr_cond_and_sums(y_col, x_col);
    let regr_sxx = sum_xx - sum_x.clone() * sum_x.clone() / n.clone();
    let regr_syy = sum_yy - sum_y.clone() * sum_y.clone() / n.clone();
    let regr_sxy = sum_xy - sum_x * sum_y / n;
    when(
        regr_sxx
            .clone()
            .gt(lit(0.0))
            .and(regr_syy.clone().gt(lit(0.0))),
    )
    .then(regr_sxy.clone() * regr_sxy / (regr_sxx * regr_syy))
    .otherwise(lit(f64::NAN))
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

/// Two-arg when(condition, value): returns value where condition is true, null otherwise (PySpark when(cond, val)).
pub fn when_then_otherwise_null(condition: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let null_expr = Expr::Literal(LiteralValue::Null);
    let expr = polars::prelude::when(condition.expr().clone())
        .then(value.expr().clone())
        .otherwise(null_expr);
    crate::column::Column::from_expr(expr, None)
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

/// Trim leading and trailing chars (PySpark btrim). trim_str defaults to whitespace.
pub fn btrim(column: &Column, trim_str: Option<&str>) -> Column {
    column.clone().btrim(trim_str)
}

/// Find substring position 1-based, starting at pos (PySpark locate). 0 if not found.
pub fn locate(substr: &str, column: &Column, pos: i64) -> Column {
    column.clone().locate(substr, pos)
}

/// Base conversion (PySpark conv). num from from_base to to_base.
pub fn conv(column: &Column, from_base: i32, to_base: i32) -> Column {
    column.clone().conv(from_base, to_base)
}

/// Convert to hex string (PySpark hex).
pub fn hex(column: &Column) -> Column {
    column.clone().hex()
}

/// Convert hex string to binary/string (PySpark unhex).
pub fn unhex(column: &Column) -> Column {
    column.clone().unhex()
}

/// Encode string to binary (PySpark encode). Charset: UTF-8. Returns hex string.
pub fn encode(column: &Column, charset: &str) -> Column {
    column.clone().encode(charset)
}

/// Decode binary (hex string) to string (PySpark decode). Charset: UTF-8.
pub fn decode(column: &Column, charset: &str) -> Column {
    column.clone().decode(charset)
}

/// Convert to binary (PySpark to_binary). fmt: 'utf-8', 'hex'.
pub fn to_binary(column: &Column, fmt: &str) -> Column {
    column.clone().to_binary(fmt)
}

/// Try convert to binary; null on failure (PySpark try_to_binary).
pub fn try_to_binary(column: &Column, fmt: &str) -> Column {
    column.clone().try_to_binary(fmt)
}

/// AES encrypt (PySpark aes_encrypt). Key as string; AES-128-GCM.
pub fn aes_encrypt(column: &Column, key: &str) -> Column {
    column.clone().aes_encrypt(key)
}

/// AES decrypt (PySpark aes_decrypt). Input hex(nonce||ciphertext).
pub fn aes_decrypt(column: &Column, key: &str) -> Column {
    column.clone().aes_decrypt(key)
}

/// Try AES decrypt (PySpark try_aes_decrypt). Returns null on failure.
pub fn try_aes_decrypt(column: &Column, key: &str) -> Column {
    column.clone().try_aes_decrypt(key)
}

/// Convert integer to binary string (PySpark bin).
pub fn bin(column: &Column) -> Column {
    column.clone().bin()
}

/// Get bit at 0-based position (PySpark getbit).
pub fn getbit(column: &Column, pos: i64) -> Column {
    column.clone().getbit(pos)
}

/// Bitwise AND of two integer/boolean columns (PySpark bit_and).
pub fn bit_and(left: &Column, right: &Column) -> Column {
    left.clone().bit_and(right)
}

/// Bitwise OR of two integer/boolean columns (PySpark bit_or).
pub fn bit_or(left: &Column, right: &Column) -> Column {
    left.clone().bit_or(right)
}

/// Bitwise XOR of two integer/boolean columns (PySpark bit_xor).
pub fn bit_xor(left: &Column, right: &Column) -> Column {
    left.clone().bit_xor(right)
}

/// Count of set bits in the integer representation (PySpark bit_count).
pub fn bit_count(column: &Column) -> Column {
    column.clone().bit_count()
}

/// Bitwise NOT of an integer/boolean column (PySpark bitwise_not / bitwiseNOT).
pub fn bitwise_not(column: &Column) -> Column {
    column.clone().bitwise_not()
}

// --- Bitmap (PySpark 3.5+) ---

/// Map integral value (0–32767) to bit position for bitmap aggregates (PySpark bitmap_bit_position).
pub fn bitmap_bit_position(column: &Column) -> Column {
    use polars::prelude::DataType;
    let expr = column.expr().clone().cast(DataType::Int32);
    Column::from_expr(expr, None)
}

/// Bucket number for distributed bitmap (PySpark bitmap_bucket_number). value / 32768.
pub fn bitmap_bucket_number(column: &Column) -> Column {
    use polars::prelude::DataType;
    let expr = column.expr().clone().cast(DataType::Int64) / lit(32768i64);
    Column::from_expr(expr, None)
}

/// Count set bits in a bitmap binary column (PySpark bitmap_count).
pub fn bitmap_count(column: &Column) -> Column {
    use polars::prelude::{DataType, GetOutput};
    let expr = column.expr().clone().map(
        crate::udfs::apply_bitmap_count,
        GetOutput::from_type(DataType::Int64),
    );
    Column::from_expr(expr, None)
}

/// Aggregate: bitwise OR of bit positions into one bitmap binary (PySpark bitmap_construct_agg).
/// Use in group_by(...).agg([bitmap_construct_agg(col)]).
pub fn bitmap_construct_agg(column: &Column) -> polars::prelude::Expr {
    use polars::prelude::{DataType, GetOutput};
    column.expr().clone().implode().map(
        crate::udfs::apply_bitmap_construct_agg,
        GetOutput::from_type(DataType::Binary),
    )
}

/// Aggregate: bitwise OR of bitmap binary column (PySpark bitmap_or_agg).
pub fn bitmap_or_agg(column: &Column) -> polars::prelude::Expr {
    use polars::prelude::{DataType, GetOutput};
    column.expr().clone().implode().map(
        crate::udfs::apply_bitmap_or_agg,
        GetOutput::from_type(DataType::Binary),
    )
}

/// Alias for getbit (PySpark bit_get).
pub fn bit_get(column: &Column, pos: i64) -> Column {
    getbit(column, pos)
}

/// Assert that all boolean values are true; errors otherwise (PySpark assert_true).
/// When err_msg is Some, it is used in the error message when assertion fails.
pub fn assert_true(column: &Column, err_msg: Option<&str>) -> Column {
    column.clone().assert_true(err_msg)
}

/// Raise an error when evaluated (PySpark raise_error). Always fails with the given message.
pub fn raise_error(message: &str) -> Column {
    let msg = message.to_string();
    let expr = lit(0i64).map(
        move |_col| -> PolarsResult<Option<polars::prelude::Column>> {
            Err(PolarsError::ComputeError(msg.clone().into()))
        },
        GetOutput::from_type(DataType::Int64),
    );
    Column::from_expr(expr, Some("raise_error".to_string()))
}

/// Broadcast hint - no-op that returns the same DataFrame (PySpark broadcast).
pub fn broadcast(df: &DataFrame) -> DataFrame {
    df.clone()
}

/// Stub partition id - always 0 (PySpark spark_partition_id).
pub fn spark_partition_id() -> Column {
    Column::from_expr(lit(0i32), Some("spark_partition_id".to_string()))
}

/// Stub input file name - empty string (PySpark input_file_name).
pub fn input_file_name() -> Column {
    Column::from_expr(lit(""), Some("input_file_name".to_string()))
}

/// Stub monotonically_increasing_id - constant 0 (PySpark monotonically_increasing_id).
/// Note: differs from PySpark which is unique per-row; see PYSPARK_DIFFERENCES.md.
pub fn monotonically_increasing_id() -> Column {
    Column::from_expr(lit(0i64), Some("monotonically_increasing_id".to_string()))
}

/// Current catalog name stub (PySpark current_catalog).
pub fn current_catalog() -> Column {
    Column::from_expr(lit("spark_catalog"), Some("current_catalog".to_string()))
}

/// Current database/schema name stub (PySpark current_database).
pub fn current_database() -> Column {
    Column::from_expr(lit("default"), Some("current_database".to_string()))
}

/// Current schema name stub (PySpark current_schema).
pub fn current_schema() -> Column {
    Column::from_expr(lit("default"), Some("current_schema".to_string()))
}

/// Current user stub (PySpark current_user).
pub fn current_user() -> Column {
    Column::from_expr(lit("unknown"), Some("current_user".to_string()))
}

/// User stub (PySpark user).
pub fn user() -> Column {
    Column::from_expr(lit("unknown"), Some("user".to_string()))
}

/// Random uniform [0, 1) per row, with optional seed (PySpark rand).
/// When added via with_column, generates one distinct value per row (PySpark-like).
pub fn rand(seed: Option<u64>) -> Column {
    Column::from_rand(seed)
}

/// Random standard normal per row, with optional seed (PySpark randn).
/// When added via with_column, generates one distinct value per row (PySpark-like).
pub fn randn(seed: Option<u64>) -> Column {
    Column::from_randn(seed)
}

/// Call a registered UDF by name. PySpark: F.call_udf(udfName, *cols).
/// Requires a session (set by get_or_create). Raises if UDF not found.
pub fn call_udf(name: &str, cols: &[Column]) -> Result<Column, PolarsError> {
    use polars::prelude::Column as PlColumn;

    let session = crate::session::get_thread_udf_session().ok_or_else(|| {
        PolarsError::InvalidOperation(
            "call_udf: no session. Use SparkSession.builder().get_or_create() first.".into(),
        )
    })?;
    let case_sensitive = session.is_case_sensitive();

    // Python UDF: eager execution via UdfCall
    #[cfg(feature = "pyo3")]
    if session
        .udf_registry
        .get_python_udf(name, case_sensitive)
        .is_some()
    {
        return Ok(Column::from_udf_call(name.to_string(), cols.to_vec()));
    }

    // Rust UDF: build lazy Expr
    let udf = session
        .udf_registry
        .get_rust_udf(name, case_sensitive)
        .ok_or_else(|| {
            PolarsError::InvalidOperation(format!("call_udf: UDF '{name}' not found").into())
        })?;

    let exprs: Vec<Expr> = cols.iter().map(|c| c.expr().clone()).collect();
    let output_type = DataType::String; // PySpark default

    let expr = if exprs.len() == 1 {
        let udf = udf.clone();
        exprs.into_iter().next().unwrap().map(
            move |c| {
                let s = c.take_materialized_series();
                udf.apply(&[s])
                    .map(|out| Some(PlColumn::new("_".into(), out)))
            },
            GetOutput::from_type(output_type),
        )
    } else {
        let udf = udf.clone();
        let first = exprs[0].clone();
        let rest: Vec<Expr> = exprs[1..].to_vec();
        first.map_many(
            move |columns| {
                let series: Vec<Series> = columns
                    .iter_mut()
                    .map(|c| std::mem::take(c).take_materialized_series())
                    .collect();
                udf.apply(&series)
                    .map(|out| Some(PlColumn::new("_".into(), out)))
            },
            &rest,
            GetOutput::from_type(output_type),
        )
    };

    Ok(Column::from_expr(expr, Some(format!("{name}()"))))
}

/// True if two arrays have any element in common (PySpark arrays_overlap).
pub fn arrays_overlap(left: &Column, right: &Column) -> Column {
    left.clone().arrays_overlap(right)
}

/// Zip arrays into array of structs (PySpark arrays_zip).
pub fn arrays_zip(left: &Column, right: &Column) -> Column {
    left.clone().arrays_zip(right)
}

/// Explode; null/empty yields one row with null (PySpark explode_outer).
pub fn explode_outer(column: &Column) -> Column {
    column.clone().explode_outer()
}

/// Posexplode with null preservation (PySpark posexplode_outer).
pub fn posexplode_outer(column: &Column) -> (Column, Column) {
    column.clone().posexplode_outer()
}

/// Collect to array (PySpark array_agg).
pub fn array_agg(column: &Column) -> Column {
    column.clone().array_agg()
}

/// Transform map keys by expr (PySpark transform_keys).
pub fn transform_keys(column: &Column, key_expr: Expr) -> Column {
    column.clone().transform_keys(key_expr)
}

/// Transform map values by expr (PySpark transform_values).
pub fn transform_values(column: &Column, value_expr: Expr) -> Column {
    column.clone().transform_values(value_expr)
}

/// Parse string to map (PySpark str_to_map). Default delims: "," and ":".
pub fn str_to_map(
    column: &Column,
    pair_delim: Option<&str>,
    key_value_delim: Option<&str>,
) -> Column {
    let pd = pair_delim.unwrap_or(",");
    let kvd = key_value_delim.unwrap_or(":");
    column.clone().str_to_map(pd, kvd)
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

/// Extract all matches of regex (PySpark regexp_extract_all).
pub fn regexp_extract_all(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_extract_all(pattern)
}

/// Check if string matches regex (PySpark regexp_like / rlike).
pub fn regexp_like(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_like(pattern)
}

/// Count of non-overlapping regex matches (PySpark regexp_count).
pub fn regexp_count(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_count(pattern)
}

/// First substring matching regex (PySpark regexp_substr). Null if no match.
pub fn regexp_substr(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_substr(pattern)
}

/// Split by delimiter and return 1-based part (PySpark split_part).
pub fn split_part(column: &Column, delimiter: &str, part_num: i64) -> Column {
    column.clone().split_part(delimiter, part_num)
}

/// 1-based position of first regex match (PySpark regexp_instr).
pub fn regexp_instr(column: &Column, pattern: &str, group_idx: Option<usize>) -> Column {
    column.clone().regexp_instr(pattern, group_idx)
}

/// 1-based index of str in comma-delimited set (PySpark find_in_set). 0 if not found or str contains comma.
pub fn find_in_set(str_column: &Column, set_column: &Column) -> Column {
    str_column.clone().find_in_set(set_column)
}

/// Printf-style format (PySpark format_string). Supports %s, %d, %i, %f, %g, %%.
pub fn format_string(format: &str, columns: &[&Column]) -> Column {
    use polars::prelude::*;
    if columns.is_empty() {
        panic!("format_string needs at least one column");
    }
    let format_owned = format.to_string();
    let args: Vec<Expr> = columns.iter().skip(1).map(|c| c.expr().clone()).collect();
    let expr = columns[0].expr().clone().map_many(
        move |cols| crate::udfs::apply_format_string(cols, &format_owned),
        &args,
        GetOutput::from_type(DataType::String),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Alias for format_string (PySpark printf).
pub fn printf(format: &str, columns: &[&Column]) -> Column {
    format_string(format, columns)
}

/// Repeat string n times (PySpark repeat).
pub fn repeat(column: &Column, n: i32) -> Column {
    column.clone().repeat(n)
}

/// Reverse string (PySpark reverse).
pub fn reverse(column: &Column) -> Column {
    column.clone().reverse()
}

/// Find substring position 1-based; 0 if not found (PySpark instr).
pub fn instr(column: &Column, substr: &str) -> Column {
    column.clone().instr(substr)
}

/// Position of substring in column (PySpark position). Same as instr; (substr, col) argument order.
pub fn position(substr: &str, column: &Column) -> Column {
    column.clone().instr(substr)
}

/// ASCII value of first character (PySpark ascii). Returns Int32.
pub fn ascii(column: &Column) -> Column {
    column.clone().ascii()
}

/// Format numeric as string with fixed decimal places (PySpark format_number).
pub fn format_number(column: &Column, decimals: u32) -> Column {
    column.clone().format_number(decimals)
}

/// Replace substring at 1-based position (PySpark overlay). replace is literal.
pub fn overlay(column: &Column, replace: &str, pos: i64, length: i64) -> Column {
    column.clone().overlay(replace, pos, length)
}

/// Int to single-character string (PySpark char). Valid codepoint only.
pub fn char(column: &Column) -> Column {
    column.clone().char()
}

/// Alias for char (PySpark chr).
pub fn chr(column: &Column) -> Column {
    column.clone().chr()
}

/// Base64 encode string bytes (PySpark base64).
pub fn base64(column: &Column) -> Column {
    column.clone().base64()
}

/// Base64 decode to string (PySpark unbase64). Invalid decode → null.
pub fn unbase64(column: &Column) -> Column {
    column.clone().unbase64()
}

/// SHA1 hash of string bytes, return hex string (PySpark sha1).
pub fn sha1(column: &Column) -> Column {
    column.clone().sha1()
}

/// SHA2 hash; bit_length 256, 384, or 512 (PySpark sha2).
pub fn sha2(column: &Column, bit_length: i32) -> Column {
    column.clone().sha2(bit_length)
}

/// MD5 hash of string bytes, return hex string (PySpark md5).
pub fn md5(column: &Column) -> Column {
    column.clone().md5()
}

/// Left-pad string to length with pad char (PySpark lpad).
pub fn lpad(column: &Column, length: i32, pad: &str) -> Column {
    column.clone().lpad(length, pad)
}

/// Right-pad string to length with pad char (PySpark rpad).
pub fn rpad(column: &Column, length: i32, pad: &str) -> Column {
    column.clone().rpad(length, pad)
}

/// Character-by-character translation (PySpark translate).
pub fn translate(column: &Column, from_str: &str, to_str: &str) -> Column {
    column.clone().translate(from_str, to_str)
}

/// Mask string: replace upper/lower/digit/other with given chars (PySpark mask).
pub fn mask(
    column: &Column,
    upper_char: Option<char>,
    lower_char: Option<char>,
    digit_char: Option<char>,
    other_char: Option<char>,
) -> Column {
    column
        .clone()
        .mask(upper_char, lower_char, digit_char, other_char)
}

/// Substring before/after nth delimiter (PySpark substring_index).
pub fn substring_index(column: &Column, delimiter: &str, count: i64) -> Column {
    column.clone().substring_index(delimiter, count)
}

/// Leftmost n characters (PySpark left).
pub fn left(column: &Column, n: i64) -> Column {
    column.clone().left(n)
}

/// Rightmost n characters (PySpark right).
pub fn right(column: &Column, n: i64) -> Column {
    column.clone().right(n)
}

/// Replace all occurrences of search with replacement (literal). PySpark replace.
pub fn replace(column: &Column, search: &str, replacement: &str) -> Column {
    column.clone().replace(search, replacement)
}

/// True if string starts with prefix (PySpark startswith).
pub fn startswith(column: &Column, prefix: &str) -> Column {
    column.clone().startswith(prefix)
}

/// True if string ends with suffix (PySpark endswith).
pub fn endswith(column: &Column, suffix: &str) -> Column {
    column.clone().endswith(suffix)
}

/// True if string contains substring (literal). PySpark contains.
pub fn contains(column: &Column, substring: &str) -> Column {
    column.clone().contains(substring)
}

/// SQL LIKE pattern (% any, _ one char). PySpark like.
/// When escape_char is Some(esc), esc + char treats that char as literal.
pub fn like(column: &Column, pattern: &str, escape_char: Option<char>) -> Column {
    column.clone().like(pattern, escape_char)
}

/// Case-insensitive LIKE. PySpark ilike.
/// When escape_char is Some(esc), esc + char treats that char as literal.
pub fn ilike(column: &Column, pattern: &str, escape_char: Option<char>) -> Column {
    column.clone().ilike(pattern, escape_char)
}

/// Alias for regexp_like. PySpark rlike / regexp.
pub fn rlike(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_like(pattern)
}

/// Alias for rlike (PySpark regexp).
pub fn regexp(column: &Column, pattern: &str) -> Column {
    rlike(column, pattern)
}

/// Soundex code (PySpark soundex). Not implemented: requires element-wise UDF.
pub fn soundex(column: &Column) -> Column {
    column.clone().soundex()
}

/// Levenshtein distance (PySpark levenshtein). Not implemented: requires element-wise UDF.
pub fn levenshtein(column: &Column, other: &Column) -> Column {
    column.clone().levenshtein(other)
}

/// CRC32 of string bytes (PySpark crc32). Not implemented: requires element-wise UDF.
pub fn crc32(column: &Column) -> Column {
    column.clone().crc32()
}

/// XXH64 hash (PySpark xxhash64). Not implemented: requires element-wise UDF.
pub fn xxhash64(column: &Column) -> Column {
    column.clone().xxhash64()
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

/// Banker's rounding - round half to even (PySpark bround).
pub fn bround(column: &Column, scale: i32) -> Column {
    column.clone().bround(scale)
}

/// Unary minus / negate (PySpark negate, negative).
pub fn negate(column: &Column) -> Column {
    column.clone().negate()
}

/// Alias for negate. PySpark negative.
pub fn negative(column: &Column) -> Column {
    negate(column)
}

/// Unary plus - no-op, returns column as-is (PySpark positive).
pub fn positive(column: &Column) -> Column {
    column.clone()
}

/// Cotangent: 1/tan (PySpark cot).
pub fn cot(column: &Column) -> Column {
    column.clone().cot()
}

/// Cosecant: 1/sin (PySpark csc).
pub fn csc(column: &Column) -> Column {
    column.clone().csc()
}

/// Secant: 1/cos (PySpark sec).
pub fn sec(column: &Column) -> Column {
    column.clone().sec()
}

/// Constant e = 2.718... (PySpark e).
pub fn e() -> Column {
    Column::from_expr(lit(std::f64::consts::E), Some("e".to_string()))
}

/// Constant pi = 3.14159... (PySpark pi).
pub fn pi() -> Column {
    Column::from_expr(lit(std::f64::consts::PI), Some("pi".to_string()))
}

/// Square root (PySpark sqrt)
pub fn sqrt(column: &Column) -> Column {
    column.clone().sqrt()
}

/// Power (PySpark pow)
pub fn pow(column: &Column, exp: i64) -> Column {
    column.clone().pow(exp)
}

/// Exponential (PySpark exp)
pub fn exp(column: &Column) -> Column {
    column.clone().exp()
}

/// Natural logarithm (PySpark log with one arg)
pub fn log(column: &Column) -> Column {
    column.clone().log()
}

/// Logarithm with given base (PySpark log(col, base)). base must be positive and not 1.
pub fn log_with_base(column: &Column, base: f64) -> Column {
    crate::column::Column::from_expr(column.expr().clone().log(base), None)
}

/// Sine in radians (PySpark sin)
pub fn sin(column: &Column) -> Column {
    column.clone().sin()
}

/// Cosine in radians (PySpark cos)
pub fn cos(column: &Column) -> Column {
    column.clone().cos()
}

/// Tangent in radians (PySpark tan)
pub fn tan(column: &Column) -> Column {
    column.clone().tan()
}

/// Arc sine (PySpark asin)
pub fn asin(column: &Column) -> Column {
    column.clone().asin()
}

/// Arc cosine (PySpark acos)
pub fn acos(column: &Column) -> Column {
    column.clone().acos()
}

/// Arc tangent (PySpark atan)
pub fn atan(column: &Column) -> Column {
    column.clone().atan()
}

/// Two-argument arc tangent atan2(y, x) in radians (PySpark atan2)
pub fn atan2(y: &Column, x: &Column) -> Column {
    y.clone().atan2(x)
}

/// Convert radians to degrees (PySpark degrees)
pub fn degrees(column: &Column) -> Column {
    column.clone().degrees()
}

/// Convert degrees to radians (PySpark radians)
pub fn radians(column: &Column) -> Column {
    column.clone().radians()
}

/// Sign of the number: -1, 0, or 1 (PySpark signum)
pub fn signum(column: &Column) -> Column {
    column.clone().signum()
}

/// Alias for signum (PySpark sign).
pub fn sign(column: &Column) -> Column {
    signum(column)
}

/// Cast column to the given type (PySpark cast). Fails on invalid conversion.
/// String-to-boolean uses custom parsing ("true"/"false"/"1"/"0") since Polars does not support Utf8->Boolean.
/// String-to-date accepts date and datetime strings (e.g. "2025-01-01 10:30:00" truncates to date) for Spark parity.
pub fn cast(column: &Column, type_name: &str) -> Result<Column, String> {
    let dtype = parse_type_name(type_name)?;
    if dtype == DataType::Boolean {
        use polars::prelude::GetOutput;
        let expr = column.expr().clone().map(
            |col| crate::udfs::apply_string_to_boolean(col, true),
            GetOutput::from_type(DataType::Boolean),
        );
        return Ok(Column::from_expr(expr, None));
    }
    if dtype == DataType::Date {
        use polars::prelude::GetOutput;
        let expr = column.expr().clone().map(
            |col| crate::udfs::apply_string_to_date(col, true),
            GetOutput::from_type(DataType::Date),
        );
        return Ok(Column::from_expr(expr, None));
    }
    if dtype == DataType::Int32 || dtype == DataType::Int64 {
        use polars::prelude::GetOutput;
        let target = dtype.clone();
        let expr = column.expr().clone().map(
            move |col| crate::udfs::apply_string_to_int(col, false, target.clone()),
            GetOutput::from_type(dtype),
        );
        return Ok(Column::from_expr(expr, None));
    }
    if dtype == DataType::Float64 {
        use polars::prelude::GetOutput;
        // String-to-double uses custom parsing for Spark-style to_number semantics.
        let expr = column.expr().clone().map(
            |col| crate::udfs::apply_string_to_double(col, true),
            GetOutput::from_type(DataType::Float64),
        );
        return Ok(Column::from_expr(expr, None));
    }
    Ok(Column::from_expr(
        column.expr().clone().strict_cast(dtype),
        None,
    ))
}

/// Cast column to the given type, returning null on invalid conversion (PySpark try_cast).
/// String-to-boolean uses custom parsing ("true"/"false"/"1"/"0") since Polars does not support Utf8->Boolean.
/// String-to-date accepts date and datetime strings; invalid strings become null.
pub fn try_cast(column: &Column, type_name: &str) -> Result<Column, String> {
    let dtype = parse_type_name(type_name)?;
    if dtype == DataType::Boolean {
        use polars::prelude::GetOutput;
        let expr = column.expr().clone().map(
            |col| crate::udfs::apply_string_to_boolean(col, false),
            GetOutput::from_type(DataType::Boolean),
        );
        return Ok(Column::from_expr(expr, None));
    }
    if dtype == DataType::Date {
        use polars::prelude::GetOutput;
        let expr = column.expr().clone().map(
            |col| crate::udfs::apply_string_to_date(col, false),
            GetOutput::from_type(DataType::Date),
        );
        return Ok(Column::from_expr(expr, None));
    }
    if dtype == DataType::Int32 || dtype == DataType::Int64 {
        use polars::prelude::GetOutput;
        let target = dtype.clone();
        let expr = column.expr().clone().map(
            move |col| crate::udfs::apply_string_to_int(col, false, target.clone()),
            GetOutput::from_type(dtype),
        );
        return Ok(Column::from_expr(expr, None));
    }
    if dtype == DataType::Float64 {
        use polars::prelude::GetOutput;
        let expr = column.expr().clone().map(
            |col| crate::udfs::apply_string_to_double(col, false),
            GetOutput::from_type(DataType::Float64),
        );
        return Ok(Column::from_expr(expr, None));
    }
    Ok(Column::from_expr(column.expr().clone().cast(dtype), None))
}

/// Cast to string, optionally with format for datetime (PySpark to_char, to_varchar).
/// When format is Some, uses date_format for datetime columns (PySpark format → chrono strftime); otherwise cast to string.
/// Returns Err if the cast to string fails (invalid type name or unsupported column type).
pub fn to_char(column: &Column, format: Option<&str>) -> Result<Column, String> {
    match format {
        Some(fmt) => Ok(column
            .clone()
            .date_format(&crate::udfs::pyspark_format_to_chrono(fmt))),
        None => cast(column, "string"),
    }
}

/// Alias for to_char (PySpark to_varchar).
pub fn to_varchar(column: &Column, format: Option<&str>) -> Result<Column, String> {
    to_char(column, format)
}

/// Cast to numeric (PySpark to_number). Uses Double. Format parameter reserved for future use.
/// Returns Err if the cast to double fails (invalid type name or unsupported column type).
pub fn to_number(column: &Column, _format: Option<&str>) -> Result<Column, String> {
    cast(column, "double")
}

/// Cast to numeric, null on invalid (PySpark try_to_number). Format parameter reserved for future use.
/// Returns Err if the try_cast setup fails (invalid type name); column values that cannot be parsed become null.
pub fn try_to_number(column: &Column, _format: Option<&str>) -> Result<Column, String> {
    try_cast(column, "double")
}

/// Cast to timestamp, or parse with format when provided (PySpark to_timestamp).
pub fn to_timestamp(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::{DataType, GetOutput, TimeUnit};
    match format {
        None => crate::cast(column, "timestamp"),
        Some(fmt) => {
            let fmt_owned = fmt.to_string();
            let expr = column.expr().clone().map(
                move |s| crate::udfs::apply_to_timestamp_format(s, Some(&fmt_owned), true),
                GetOutput::from_type(DataType::Datetime(TimeUnit::Microseconds, None)),
            );
            Ok(crate::column::Column::from_expr(expr, None))
        }
    }
}

/// Cast to timestamp, null on invalid, or parse with format when provided (PySpark try_to_timestamp).
/// Returns Err if the try_cast setup fails (invalid type name) when format is None.
pub fn try_to_timestamp(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::*;
    match format {
        None => try_cast(column, "timestamp"),
        Some(fmt) => {
            let fmt_owned = fmt.to_string();
            let expr = column.expr().clone().map(
                move |s| crate::udfs::apply_to_timestamp_format(s, Some(&fmt_owned), false),
                GetOutput::from_type(DataType::Datetime(TimeUnit::Microseconds, None)),
            );
            Ok(crate::column::Column::from_expr(expr, None))
        }
    }
}

/// Parse as timestamp in local timezone, return UTC (PySpark to_timestamp_ltz).
pub fn to_timestamp_ltz(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::{DataType, GetOutput, TimeUnit};
    match format {
        None => crate::cast(column, "timestamp"),
        Some(fmt) => {
            let fmt_owned = fmt.to_string();
            let expr = column.expr().clone().map(
                move |s| crate::udfs::apply_to_timestamp_ltz_format(s, Some(&fmt_owned), true),
                GetOutput::from_type(DataType::Datetime(TimeUnit::Microseconds, None)),
            );
            Ok(crate::column::Column::from_expr(expr, None))
        }
    }
}

/// Parse as timestamp without timezone (PySpark to_timestamp_ntz). Returns Datetime(_, None).
pub fn to_timestamp_ntz(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::{DataType, GetOutput, TimeUnit};
    match format {
        None => crate::cast(column, "timestamp"),
        Some(fmt) => {
            let fmt_owned = fmt.to_string();
            let expr = column.expr().clone().map(
                move |s| crate::udfs::apply_to_timestamp_ntz_format(s, Some(&fmt_owned), true),
                GetOutput::from_type(DataType::Datetime(TimeUnit::Microseconds, None)),
            );
            Ok(crate::column::Column::from_expr(expr, None))
        }
    }
}

/// Division that returns null on divide-by-zero (PySpark try_divide).
pub fn try_divide(left: &Column, right: &Column) -> Column {
    use polars::prelude::*;
    let zero_cond = right.expr().clone().cast(DataType::Float64).eq(lit(0.0f64));
    let null_expr = Expr::Literal(LiteralValue::Null);
    let div_expr =
        left.expr().clone().cast(DataType::Float64) / right.expr().clone().cast(DataType::Float64);
    let expr = polars::prelude::when(zero_cond)
        .then(null_expr)
        .otherwise(div_expr);
    crate::column::Column::from_expr(expr, None)
}

/// Add that returns null on overflow (PySpark try_add). Uses checked arithmetic.
pub fn try_add(left: &Column, right: &Column) -> Column {
    let args = [right.expr().clone()];
    let expr =
        left.expr()
            .clone()
            .map_many(crate::udfs::apply_try_add, &args, GetOutput::same_type());
    Column::from_expr(expr, None)
}

/// Subtract that returns null on overflow (PySpark try_subtract).
pub fn try_subtract(left: &Column, right: &Column) -> Column {
    let args = [right.expr().clone()];
    let expr = left.expr().clone().map_many(
        crate::udfs::apply_try_subtract,
        &args,
        GetOutput::same_type(),
    );
    Column::from_expr(expr, None)
}

/// Multiply that returns null on overflow (PySpark try_multiply).
pub fn try_multiply(left: &Column, right: &Column) -> Column {
    let args = [right.expr().clone()];
    let expr = left.expr().clone().map_many(
        crate::udfs::apply_try_multiply,
        &args,
        GetOutput::same_type(),
    );
    Column::from_expr(expr, None)
}

/// Element at index, null if out of bounds (PySpark try_element_at). Same as element_at for lists.
pub fn try_element_at(column: &Column, index: i64) -> Column {
    column.clone().element_at(index)
}

/// Assign value to histogram bucket (PySpark width_bucket). Returns 0 if v < min_val, num_bucket+1 if v >= max_val.
pub fn width_bucket(value: &Column, min_val: f64, max_val: f64, num_bucket: i64) -> Column {
    if num_bucket <= 0 {
        panic!(
            "width_bucket: num_bucket must be positive, got {}",
            num_bucket
        );
    }
    use polars::prelude::*;
    let v = value.expr().clone().cast(DataType::Float64);
    let min_expr = lit(min_val);
    let max_expr = lit(max_val);
    let nb = num_bucket as f64;
    let width = (max_val - min_val) / nb;
    let bucket_expr = (v.clone() - min_expr.clone()) / lit(width);
    let floor_bucket = bucket_expr.floor().cast(DataType::Int64) + lit(1i64);
    let bucket_clamped = floor_bucket.clip(lit(1i64), lit(num_bucket));
    let expr = polars::prelude::when(v.clone().lt(min_expr))
        .then(lit(0i64))
        .when(v.gt_eq(max_expr))
        .then(lit(num_bucket + 1))
        .otherwise(bucket_clamped);
    crate::column::Column::from_expr(expr, None)
}

/// Return column at 1-based index (PySpark elt). elt(2, a, b, c) returns b.
pub fn elt(index: &Column, columns: &[&Column]) -> Column {
    use polars::prelude::*;
    if columns.is_empty() {
        panic!("elt requires at least one column");
    }
    let idx_expr = index.expr().clone();
    let null_expr = Expr::Literal(LiteralValue::Null);
    let mut expr = null_expr;
    for (i, c) in columns.iter().enumerate().rev() {
        let n = (i + 1) as i64;
        expr = polars::prelude::when(idx_expr.clone().eq(lit(n)))
            .then(c.expr().clone())
            .otherwise(expr);
    }
    crate::column::Column::from_expr(expr, None)
}

/// Bit length of string (bytes * 8) (PySpark bit_length).
pub fn bit_length(column: &Column) -> Column {
    column.clone().bit_length()
}

/// Length of string in bytes (PySpark octet_length).
pub fn octet_length(column: &Column) -> Column {
    column.clone().octet_length()
}

/// Length of string in characters (PySpark char_length). Alias of length().
pub fn char_length(column: &Column) -> Column {
    column.clone().char_length()
}

/// Length of string in characters (PySpark character_length). Alias of length().
pub fn character_length(column: &Column) -> Column {
    column.clone().character_length()
}

/// Data type of column as string (PySpark typeof). Constant per column from schema.
pub fn typeof_(column: &Column) -> Column {
    column.clone().typeof_()
}

/// True where the float value is NaN (PySpark isnan).
pub fn isnan(column: &Column) -> Column {
    column.clone().is_nan()
}

/// Greatest of the given columns per row (PySpark greatest). Uses element-wise UDF.
pub fn greatest(columns: &[&Column]) -> Result<Column, String> {
    if columns.is_empty() {
        return Err("greatest requires at least one column".to_string());
    }
    if columns.len() == 1 {
        return Ok((*columns[0]).clone());
    }
    let mut expr = columns[0].expr().clone();
    for c in columns.iter().skip(1) {
        let args = [c.expr().clone()];
        expr = expr.map_many(crate::udfs::apply_greatest2, &args, GetOutput::same_type());
    }
    Ok(Column::from_expr(expr, None))
}

/// Least of the given columns per row (PySpark least). Uses element-wise UDF.
pub fn least(columns: &[&Column]) -> Result<Column, String> {
    if columns.is_empty() {
        return Err("least requires at least one column".to_string());
    }
    if columns.len() == 1 {
        return Ok((*columns[0]).clone());
    }
    let mut expr = columns[0].expr().clone();
    for c in columns.iter().skip(1) {
        let args = [c.expr().clone()];
        expr = expr.map_many(crate::udfs::apply_least2, &args, GetOutput::same_type());
    }
    Ok(Column::from_expr(expr, None))
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

/// Cast to date (PySpark to_date)
pub fn to_date(column: &Column) -> Column {
    column.clone().to_date()
}

/// Format date/datetime as string (PySpark date_format). Accepts PySpark/Java SimpleDateFormat style (e.g. "yyyy-MM") and converts to chrono strftime internally.
pub fn date_format(column: &Column, format: &str) -> Column {
    column
        .clone()
        .date_format(&crate::udfs::pyspark_format_to_chrono(format))
}

/// Current date (evaluation time). PySpark current_date.
pub fn current_date() -> Column {
    use polars::prelude::*;
    let today = chrono::Utc::now().date_naive();
    let days = (today - crate::date_utils::epoch_naive_date()).num_days() as i32;
    crate::column::Column::from_expr(Expr::Literal(LiteralValue::Date(days)), None)
}

/// Current timestamp (evaluation time). PySpark current_timestamp.
pub fn current_timestamp() -> Column {
    use polars::prelude::*;
    let ts = chrono::Utc::now().timestamp_micros();
    crate::column::Column::from_expr(
        Expr::Literal(LiteralValue::DateTime(ts, TimeUnit::Microseconds, None)),
        None,
    )
}

/// Alias for current_date (PySpark curdate).
pub fn curdate() -> Column {
    current_date()
}

/// Alias for current_timestamp (PySpark now).
pub fn now() -> Column {
    current_timestamp()
}

/// Alias for current_timestamp (PySpark localtimestamp).
pub fn localtimestamp() -> Column {
    current_timestamp()
}

/// Alias for datediff (PySpark date_diff). date_diff(end, start).
pub fn date_diff(end: &Column, start: &Column) -> Column {
    datediff(end, start)
}

/// Alias for date_add (PySpark dateadd).
pub fn dateadd(column: &Column, n: i32) -> Column {
    date_add(column, n)
}

/// Extract field from date/datetime (PySpark extract). field: year, month, day, hour, minute, second, quarter, week, dayofweek, dayofyear.
pub fn extract(column: &Column, field: &str) -> Column {
    column.clone().extract(field)
}

/// Alias for extract (PySpark date_part).
pub fn date_part(column: &Column, field: &str) -> Column {
    extract(column, field)
}

/// Alias for extract (PySpark datepart).
pub fn datepart(column: &Column, field: &str) -> Column {
    extract(column, field)
}

/// Timestamp to microseconds since epoch (PySpark unix_micros).
pub fn unix_micros(column: &Column) -> Column {
    column.clone().unix_micros()
}

/// Timestamp to milliseconds since epoch (PySpark unix_millis).
pub fn unix_millis(column: &Column) -> Column {
    column.clone().unix_millis()
}

/// Timestamp to seconds since epoch (PySpark unix_seconds).
pub fn unix_seconds(column: &Column) -> Column {
    column.clone().unix_seconds()
}

/// Weekday name "Mon","Tue",... (PySpark dayname).
pub fn dayname(column: &Column) -> Column {
    column.clone().dayname()
}

/// Weekday 0=Mon, 6=Sun (PySpark weekday).
pub fn weekday(column: &Column) -> Column {
    column.clone().weekday()
}

/// Extract hour from datetime column (PySpark hour).
pub fn hour(column: &Column) -> Column {
    column.clone().hour()
}

/// Extract minute from datetime column (PySpark minute).
pub fn minute(column: &Column) -> Column {
    column.clone().minute()
}

/// Extract second from datetime column (PySpark second).
pub fn second(column: &Column) -> Column {
    column.clone().second()
}

/// Add n days to date column (PySpark date_add).
pub fn date_add(column: &Column, n: i32) -> Column {
    column.clone().date_add(n)
}

/// Subtract n days from date column (PySpark date_sub).
pub fn date_sub(column: &Column, n: i32) -> Column {
    column.clone().date_sub(n)
}

/// Number of days between two date columns (PySpark datediff).
pub fn datediff(end: &Column, start: &Column) -> Column {
    start.clone().datediff(end)
}

/// Last day of month for date column (PySpark last_day).
pub fn last_day(column: &Column) -> Column {
    column.clone().last_day()
}

/// Truncate date/datetime to unit (PySpark trunc).
pub fn trunc(column: &Column, format: &str) -> Column {
    column.clone().trunc(format)
}

/// Alias for trunc (PySpark date_trunc).
pub fn date_trunc(format: &str, column: &Column) -> Column {
    trunc(column, format)
}

/// Extract quarter (1-4) from date/datetime (PySpark quarter).
pub fn quarter(column: &Column) -> Column {
    column.clone().quarter()
}

/// Extract ISO week of year (1-53) (PySpark weekofyear).
pub fn weekofyear(column: &Column) -> Column {
    column.clone().weekofyear()
}

/// Extract day of week: 1=Sunday..7=Saturday (PySpark dayofweek).
pub fn dayofweek(column: &Column) -> Column {
    column.clone().dayofweek()
}

/// Extract day of year (1-366) (PySpark dayofyear).
pub fn dayofyear(column: &Column) -> Column {
    column.clone().dayofyear()
}

/// Add n months to date column (PySpark add_months).
pub fn add_months(column: &Column, n: i32) -> Column {
    column.clone().add_months(n)
}

/// Months between end and start dates as fractional (PySpark months_between).
/// When round_off is true, rounds to 8 decimal places (PySpark default).
pub fn months_between(end: &Column, start: &Column, round_off: bool) -> Column {
    end.clone().months_between(start, round_off)
}

/// Next date that is the given weekday (e.g. "Mon") (PySpark next_day).
pub fn next_day(column: &Column, day_of_week: &str) -> Column {
    column.clone().next_day(day_of_week)
}

/// Current Unix timestamp in seconds (PySpark unix_timestamp with no args).
pub fn unix_timestamp_now() -> Column {
    use polars::prelude::*;
    let secs = chrono::Utc::now().timestamp();
    crate::column::Column::from_expr(lit(secs), None)
}

/// Parse string timestamp to seconds since epoch (PySpark unix_timestamp). format defaults to yyyy-MM-dd HH:mm:ss.
pub fn unix_timestamp(column: &Column, format: Option<&str>) -> Column {
    column.clone().unix_timestamp(format)
}

/// Alias for unix_timestamp.
pub fn to_unix_timestamp(column: &Column, format: Option<&str>) -> Column {
    unix_timestamp(column, format)
}

/// Convert seconds since epoch to formatted string (PySpark from_unixtime).
pub fn from_unixtime(column: &Column, format: Option<&str>) -> Column {
    column.clone().from_unixtime(format)
}

/// Build date from year, month, day columns (PySpark make_date).
pub fn make_date(year: &Column, month: &Column, day: &Column) -> Column {
    use polars::prelude::*;
    let args = [month.expr().clone(), day.expr().clone()];
    let expr = year.expr().clone().map_many(
        crate::udfs::apply_make_date,
        &args,
        GetOutput::from_type(DataType::Date),
    );
    crate::column::Column::from_expr(expr, None)
}

/// make_timestamp(year, month, day, hour, min, sec, timezone?) - six columns to timestamp (PySpark make_timestamp).
/// When timezone is Some(tz), components are interpreted as local time in that zone, then converted to UTC.
pub fn make_timestamp(
    year: &Column,
    month: &Column,
    day: &Column,
    hour: &Column,
    minute: &Column,
    sec: &Column,
    timezone: Option<&str>,
) -> Column {
    use polars::prelude::*;
    let tz_owned = timezone.map(|s| s.to_string());
    let args = [
        month.expr().clone(),
        day.expr().clone(),
        hour.expr().clone(),
        minute.expr().clone(),
        sec.expr().clone(),
    ];
    let expr = year.expr().clone().map_many(
        move |cols| crate::udfs::apply_make_timestamp(cols, tz_owned.as_deref()),
        &args,
        GetOutput::from_type(DataType::Datetime(TimeUnit::Microseconds, None)),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Add amount of unit to timestamp (PySpark timestampadd).
pub fn timestampadd(unit: &str, amount: &Column, ts: &Column) -> Column {
    ts.clone().timestampadd(unit, amount)
}

/// Difference between timestamps in unit (PySpark timestampdiff).
pub fn timestampdiff(unit: &str, start: &Column, end: &Column) -> Column {
    start.clone().timestampdiff(unit, end)
}

/// Interval of n days (PySpark days). For use in date_add, timestampadd, etc.
pub fn days(n: i64) -> Column {
    make_interval(0, 0, 0, n, 0, 0, 0)
}

/// Interval of n hours (PySpark hours).
pub fn hours(n: i64) -> Column {
    make_interval(0, 0, 0, 0, n, 0, 0)
}

/// Interval of n minutes (PySpark minutes).
pub fn minutes(n: i64) -> Column {
    make_interval(0, 0, 0, 0, 0, n, 0)
}

/// Interval of n months (PySpark months). Approximated as 30*n days.
pub fn months(n: i64) -> Column {
    make_interval(0, n, 0, 0, 0, 0, 0)
}

/// Interval of n years (PySpark years). Approximated as 365*n days.
pub fn years(n: i64) -> Column {
    make_interval(n, 0, 0, 0, 0, 0, 0)
}

/// Interpret timestamp as UTC, convert to tz (PySpark from_utc_timestamp).
pub fn from_utc_timestamp(column: &Column, tz: &str) -> Column {
    column.clone().from_utc_timestamp(tz)
}

/// Interpret timestamp as in tz, convert to UTC (PySpark to_utc_timestamp).
pub fn to_utc_timestamp(column: &Column, tz: &str) -> Column {
    column.clone().to_utc_timestamp(tz)
}

/// Convert timestamp between timezones (PySpark convert_timezone).
pub fn convert_timezone(source_tz: &str, target_tz: &str, column: &Column) -> Column {
    let source_tz = source_tz.to_string();
    let target_tz = target_tz.to_string();
    let expr = column.expr().clone().map(
        move |s| crate::udfs::apply_convert_timezone(s, &source_tz, &target_tz),
        GetOutput::same_type(),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Current session timezone (PySpark current_timezone). Default "UTC". Returns literal column.
pub fn current_timezone() -> Column {
    use polars::prelude::*;
    crate::column::Column::from_expr(lit("UTC"), None)
}

/// Create interval duration (PySpark make_interval). Optional args; 0 for omitted.
pub fn make_interval(
    years: i64,
    months: i64,
    weeks: i64,
    days: i64,
    hours: i64,
    mins: i64,
    secs: i64,
) -> Column {
    use polars::prelude::*;
    // Approximate: 1 year = 365 days, 1 month = 30 days
    let total_days = years * 365 + months * 30 + weeks * 7 + days;
    let args = DurationArgs::new()
        .with_days(lit(total_days))
        .with_hours(lit(hours))
        .with_minutes(lit(mins))
        .with_seconds(lit(secs));
    let dur = duration(args);
    crate::column::Column::from_expr(dur, None)
}

/// Day-time interval: days, hours, minutes, seconds (PySpark make_dt_interval). All optional; 0 for omitted.
pub fn make_dt_interval(days: i64, hours: i64, minutes: i64, seconds: i64) -> Column {
    use polars::prelude::*;
    let args = DurationArgs::new()
        .with_days(lit(days))
        .with_hours(lit(hours))
        .with_minutes(lit(minutes))
        .with_seconds(lit(seconds));
    let dur = duration(args);
    crate::column::Column::from_expr(dur, None)
}

/// Year-month interval (PySpark make_ym_interval). Polars has no native YM type; return months as Int32 (years*12 + months).
pub fn make_ym_interval(years: i32, months: i32) -> Column {
    use polars::prelude::*;
    let total_months = years * 12 + months;
    crate::column::Column::from_expr(lit(total_months), None)
}

/// Alias for make_timestamp (PySpark make_timestamp_ntz - no timezone).
pub fn make_timestamp_ntz(
    year: &Column,
    month: &Column,
    day: &Column,
    hour: &Column,
    minute: &Column,
    sec: &Column,
) -> Column {
    make_timestamp(year, month, day, hour, minute, sec, None)
}

/// Convert seconds since epoch to timestamp (PySpark timestamp_seconds).
pub fn timestamp_seconds(column: &Column) -> Column {
    column.clone().timestamp_seconds()
}

/// Convert milliseconds since epoch to timestamp (PySpark timestamp_millis).
pub fn timestamp_millis(column: &Column) -> Column {
    column.clone().timestamp_millis()
}

/// Convert microseconds since epoch to timestamp (PySpark timestamp_micros).
pub fn timestamp_micros(column: &Column) -> Column {
    column.clone().timestamp_micros()
}

/// Date to days since 1970-01-01 (PySpark unix_date).
pub fn unix_date(column: &Column) -> Column {
    column.clone().unix_date()
}

/// Days since epoch to date (PySpark date_from_unix_date).
pub fn date_from_unix_date(column: &Column) -> Column {
    column.clone().date_from_unix_date()
}

/// Positive modulus (PySpark pmod).
pub fn pmod(dividend: &Column, divisor: &Column) -> Column {
    dividend.clone().pmod(divisor)
}

/// Factorial n! (PySpark factorial). n in 0..=20; null for negative or overflow.
pub fn factorial(column: &Column) -> Column {
    column.clone().factorial()
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

/// Alias for coalesce(col, value). PySpark nvl / ifnull.
pub fn nvl(column: &Column, value: &Column) -> Column {
    coalesce(&[column, value])
}

/// Alias for nvl. PySpark ifnull.
pub fn ifnull(column: &Column, value: &Column) -> Column {
    nvl(column, value)
}

/// Return null if column equals value, else column. PySpark nullif.
pub fn nullif(column: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let cond = column.expr().clone().eq(value.expr().clone());
    let null_lit = Expr::Literal(LiteralValue::Null);
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
    day(column)
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

/// Create an array column from multiple columns (PySpark array).
pub fn array(columns: &[&Column]) -> Result<crate::column::Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::ComputeError(
            "array requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let expr = concat_list(exprs)
        .map_err(|e| PolarsError::ComputeError(format!("array concat_list: {e}").into()))?;
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Number of elements in list (PySpark size / array_size). Returns Int32.
pub fn array_size(column: &Column) -> Column {
    column.clone().array_size()
}

/// Alias for array_size (PySpark size).
pub fn size(column: &Column) -> Column {
    column.clone().array_size()
}

/// Cardinality: number of elements in array (PySpark cardinality). Alias for size/array_size.
pub fn cardinality(column: &Column) -> Column {
    column.clone().cardinality()
}

/// Check if list contains value (PySpark array_contains).
pub fn array_contains(column: &Column, value: &Column) -> Column {
    column.clone().array_contains(value.expr().clone())
}

/// Join list of strings with separator (PySpark array_join).
pub fn array_join(column: &Column, separator: &str) -> Column {
    column.clone().array_join(separator)
}

/// Maximum element in list (PySpark array_max).
pub fn array_max(column: &Column) -> Column {
    column.clone().array_max()
}

/// Minimum element in list (PySpark array_min).
pub fn array_min(column: &Column) -> Column {
    column.clone().array_min()
}

/// Get element at 1-based index (PySpark element_at).
pub fn element_at(column: &Column, index: i64) -> Column {
    column.clone().element_at(index)
}

/// Sort list elements (PySpark array_sort).
pub fn array_sort(column: &Column) -> Column {
    column.clone().array_sort()
}

/// Distinct elements in list (PySpark array_distinct).
pub fn array_distinct(column: &Column) -> Column {
    column.clone().array_distinct()
}

/// Slice list from 1-based start with optional length (PySpark slice).
pub fn array_slice(column: &Column, start: i64, length: Option<i64>) -> Column {
    column.clone().array_slice(start, length)
}

/// Generate array of numbers from start to stop (inclusive) with optional step (PySpark sequence).
/// step defaults to 1.
pub fn sequence(start: &Column, stop: &Column, step: Option<&Column>) -> Column {
    use polars::prelude::{as_struct, lit, DataType, GetOutput};
    let step_expr = step
        .map(|c| c.expr().clone().alias("2"))
        .unwrap_or_else(|| lit(1i64).alias("2"));
    let struct_expr = as_struct(vec![
        start.expr().clone().alias("0"),
        stop.expr().clone().alias("1"),
        step_expr,
    ]);
    let out_dtype = DataType::List(Box::new(DataType::Int64));
    let expr = struct_expr.map(crate::udfs::apply_sequence, GetOutput::from_type(out_dtype));
    crate::column::Column::from_expr(expr, None)
}

/// Random permutation of list elements (PySpark shuffle).
pub fn shuffle(column: &Column) -> Column {
    use polars::prelude::GetOutput;
    let expr = column
        .expr()
        .clone()
        .map(crate::udfs::apply_shuffle, GetOutput::same_type());
    crate::column::Column::from_expr(expr, None)
}

/// Explode list of structs into rows; struct fields become columns after unnest (PySpark inline).
/// Returns the exploded struct column; use unnest to expand struct fields to columns.
pub fn inline(column: &Column) -> Column {
    column.clone().explode()
}

/// Like inline but null/empty yields one row of nulls (PySpark inline_outer).
pub fn inline_outer(column: &Column) -> Column {
    column.clone().explode_outer()
}

/// Explode list into one row per element (PySpark explode).
pub fn explode(column: &Column) -> Column {
    column.clone().explode()
}

/// 1-based index of first occurrence of value in list, or 0 if not found (PySpark array_position).
/// Implemented via Polars list.eval with col("") as element.
pub fn array_position(column: &Column, value: &Column) -> Column {
    column.clone().array_position(value.expr().clone())
}

/// Remove null elements from list (PySpark array_compact).
pub fn array_compact(column: &Column) -> Column {
    column.clone().array_compact()
}

/// New list with all elements equal to value removed (PySpark array_remove).
/// Implemented via Polars list.eval + list.drop_nulls.
pub fn array_remove(column: &Column, value: &Column) -> Column {
    column.clone().array_remove(value.expr().clone())
}

/// Repeat each element n times (PySpark array_repeat). Not implemented: would require list.eval with dynamic repeat.
pub fn array_repeat(column: &Column, n: i64) -> Column {
    column.clone().array_repeat(n)
}

/// Flatten list of lists to one list (PySpark flatten). Not implemented.
pub fn array_flatten(column: &Column) -> Column {
    column.clone().array_flatten()
}

/// True if any list element satisfies the predicate (PySpark exists).
pub fn array_exists(column: &Column, predicate: Expr) -> Column {
    column.clone().array_exists(predicate)
}

/// True if all list elements satisfy the predicate (PySpark forall).
pub fn array_forall(column: &Column, predicate: Expr) -> Column {
    column.clone().array_forall(predicate)
}

/// Filter list elements by predicate (PySpark filter).
pub fn array_filter(column: &Column, predicate: Expr) -> Column {
    column.clone().array_filter(predicate)
}

/// Transform list elements by expression (PySpark transform).
pub fn array_transform(column: &Column, f: Expr) -> Column {
    column.clone().array_transform(f)
}

/// Sum of list elements (PySpark aggregate sum).
pub fn array_sum(column: &Column) -> Column {
    column.clone().array_sum()
}

/// Array fold/aggregate (PySpark aggregate). Simplified: zero + sum(list elements).
pub fn aggregate(column: &Column, zero: &Column) -> Column {
    column.clone().array_aggregate(zero)
}

/// Mean of list elements (PySpark aggregate avg).
pub fn array_mean(column: &Column) -> Column {
    column.clone().array_mean()
}

/// Explode list with position (PySpark posexplode). Returns (pos_column, value_column).
/// pos is 1-based; implemented via list.eval(cum_count()).explode() and explode().
pub fn posexplode(column: &Column) -> (Column, Column) {
    column.clone().posexplode()
}

/// Build a map column from alternating key/value expressions (PySpark create_map).
/// Returns List(Struct{key, value}) using Polars as_struct and concat_list.
pub fn create_map(key_values: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::{as_struct, concat_list};
    if key_values.is_empty() {
        return Err(PolarsError::ComputeError(
            "create_map requires at least one key-value pair".into(),
        ));
    }
    let mut struct_exprs: Vec<Expr> = Vec::new();
    for i in (0..key_values.len()).step_by(2) {
        if i + 1 < key_values.len() {
            let k = key_values[i].expr().clone().alias("key");
            let v = key_values[i + 1].expr().clone().alias("value");
            struct_exprs.push(as_struct(vec![k, v]));
        }
    }
    let expr = concat_list(struct_exprs)
        .map_err(|e| PolarsError::ComputeError(format!("create_map concat_list: {e}").into()))?;
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Extract keys from a map column (PySpark map_keys). Map is List(Struct{key, value}).
pub fn map_keys(column: &Column) -> Column {
    column.clone().map_keys()
}

/// Extract values from a map column (PySpark map_values).
pub fn map_values(column: &Column) -> Column {
    column.clone().map_values()
}

/// Return map as list of structs {key, value} (PySpark map_entries).
pub fn map_entries(column: &Column) -> Column {
    column.clone().map_entries()
}

/// Build map from two array columns keys and values (PySpark map_from_arrays). Implemented via UDF.
pub fn map_from_arrays(keys: &Column, values: &Column) -> Column {
    keys.clone().map_from_arrays(values)
}

/// Merge two map columns (PySpark map_concat). Last value wins for duplicate keys.
pub fn map_concat(a: &Column, b: &Column) -> Column {
    a.clone().map_concat(b)
}

/// Array of structs {key, value} to map (PySpark map_from_entries).
pub fn map_from_entries(column: &Column) -> Column {
    column.clone().map_from_entries()
}

/// True if map contains key (PySpark map_contains_key).
pub fn map_contains_key(map_col: &Column, key: &Column) -> Column {
    map_col.clone().map_contains_key(key)
}

/// Get value for key from map, or null (PySpark get).
pub fn get(map_col: &Column, key: &Column) -> Column {
    map_col.clone().get(key)
}

/// Filter map entries by predicate (PySpark map_filter).
pub fn map_filter(map_col: &Column, predicate: Expr) -> Column {
    map_col.clone().map_filter(predicate)
}

/// Merge two maps by key with merge function (PySpark map_zip_with).
pub fn map_zip_with(map1: &Column, map2: &Column, merge: Expr) -> Column {
    map1.clone().map_zip_with(map2, merge)
}

/// Convenience: zip_with with coalesce(left, right) merge.
pub fn zip_with_coalesce(left: &Column, right: &Column) -> Column {
    use polars::prelude::col;
    let left_field = col("").struct_().field_by_name("left");
    let right_field = col("").struct_().field_by_name("right");
    let merge = crate::column::Column::from_expr(
        coalesce(&[
            &crate::column::Column::from_expr(left_field, None),
            &crate::column::Column::from_expr(right_field, None),
        ])
        .into_expr(),
        None,
    );
    left.clone().zip_with(right, merge.into_expr())
}

/// Convenience: map_zip_with with coalesce(value1, value2) merge.
pub fn map_zip_with_coalesce(map1: &Column, map2: &Column) -> Column {
    use polars::prelude::col;
    let v1 = col("").struct_().field_by_name("value1");
    let v2 = col("").struct_().field_by_name("value2");
    let merge = coalesce(&[
        &crate::column::Column::from_expr(v1, None),
        &crate::column::Column::from_expr(v2, None),
    ])
    .into_expr();
    map1.clone().map_zip_with(map2, merge)
}

/// Convenience: map_filter with value > threshold predicate.
pub fn map_filter_value_gt(map_col: &Column, threshold: f64) -> Column {
    use polars::prelude::{col, lit};
    let pred = col("").struct_().field_by_name("value").gt(lit(threshold));
    map_col.clone().map_filter(pred)
}

/// Create struct from columns using column names as field names (PySpark struct).
pub fn struct_(columns: &[&Column]) -> Column {
    use polars::prelude::as_struct;
    if columns.is_empty() {
        panic!("struct requires at least one column");
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    crate::column::Column::from_expr(as_struct(exprs), None)
}

/// Create struct with explicit field names (PySpark named_struct). Pairs of (name, column).
pub fn named_struct(pairs: &[(&str, &Column)]) -> Column {
    use polars::prelude::as_struct;
    if pairs.is_empty() {
        panic!("named_struct requires at least one (name, column) pair");
    }
    let exprs: Vec<Expr> = pairs
        .iter()
        .map(|(name, col)| col.expr().clone().alias(*name))
        .collect();
    crate::column::Column::from_expr(as_struct(exprs), None)
}

/// Append element to end of list (PySpark array_append).
pub fn array_append(array: &Column, elem: &Column) -> Column {
    array.clone().array_append(elem)
}

/// Prepend element to start of list (PySpark array_prepend).
pub fn array_prepend(array: &Column, elem: &Column) -> Column {
    array.clone().array_prepend(elem)
}

/// Insert element at 1-based position (PySpark array_insert).
pub fn array_insert(array: &Column, pos: &Column, elem: &Column) -> Column {
    array.clone().array_insert(pos, elem)
}

/// Elements in first array not in second (PySpark array_except).
pub fn array_except(a: &Column, b: &Column) -> Column {
    a.clone().array_except(b)
}

/// Elements in both arrays (PySpark array_intersect).
pub fn array_intersect(a: &Column, b: &Column) -> Column {
    a.clone().array_intersect(b)
}

/// Distinct elements from both arrays (PySpark array_union).
pub fn array_union(a: &Column, b: &Column) -> Column {
    a.clone().array_union(b)
}

/// Zip two arrays element-wise with merge function (PySpark zip_with).
pub fn zip_with(left: &Column, right: &Column, merge: Expr) -> Column {
    left.clone().zip_with(right, merge)
}

/// Extract JSON path from string column (PySpark get_json_object).
pub fn get_json_object(column: &Column, path: &str) -> Column {
    column.clone().get_json_object(path)
}

/// Keys of JSON object (PySpark json_object_keys). Returns list of strings.
pub fn json_object_keys(column: &Column) -> Column {
    column.clone().json_object_keys()
}

/// Extract keys from JSON as struct (PySpark json_tuple). keys: e.g. ["a", "b"].
pub fn json_tuple(column: &Column, keys: &[&str]) -> Column {
    column.clone().json_tuple(keys)
}

/// Parse CSV string to struct (PySpark from_csv). Minimal implementation.
pub fn from_csv(column: &Column) -> Column {
    column.clone().from_csv()
}

/// Format struct as CSV string (PySpark to_csv). Minimal implementation.
pub fn to_csv(column: &Column) -> Column {
    column.clone().to_csv()
}

/// Schema of CSV string (PySpark schema_of_csv). Returns literal schema string; minimal stub.
pub fn schema_of_csv(_column: &Column) -> Column {
    Column::from_expr(
        lit("STRUCT<_c0: STRING, _c1: STRING>".to_string()),
        Some("schema_of_csv".to_string()),
    )
}

/// Schema of JSON string (PySpark schema_of_json). Returns literal schema string; minimal stub.
pub fn schema_of_json(_column: &Column) -> Column {
    Column::from_expr(
        lit("STRUCT<>".to_string()),
        Some("schema_of_json".to_string()),
    )
}

/// Parse string column as JSON into struct (PySpark from_json).
pub fn from_json(column: &Column, schema: Option<polars::datatypes::DataType>) -> Column {
    column.clone().from_json(schema)
}

/// Serialize struct column to JSON string (PySpark to_json).
pub fn to_json(column: &Column) -> Column {
    column.clone().to_json()
}

/// Check if column values are in the given list (PySpark isin). Uses Polars is_in.
pub fn isin(column: &Column, other: &Column) -> Column {
    column.clone().isin(other)
}

/// Check if column values are in the given i64 slice (PySpark isin with literal list).
pub fn isin_i64(column: &Column, values: &[i64]) -> Column {
    let s = Series::from_iter(values.iter().cloned());
    Column::from_expr(column.expr().clone().is_in(lit(s)), None)
}

/// Check if column values are in the given string slice (PySpark isin with literal list).
pub fn isin_str(column: &Column, values: &[&str]) -> Column {
    let s: Series = Series::from_iter(values.iter().copied());
    Column::from_expr(column.expr().clone().is_in(lit(s)), None)
}

/// Percent-decode URL-encoded string (PySpark url_decode).
pub fn url_decode(column: &Column) -> Column {
    column.clone().url_decode()
}

/// Percent-encode string for URL (PySpark url_encode).
pub fn url_encode(column: &Column) -> Column {
    column.clone().url_encode()
}

/// Bitwise left shift (PySpark shiftLeft). col << n.
pub fn shift_left(column: &Column, n: i32) -> Column {
    column.clone().shift_left(n)
}

/// Bitwise signed right shift (PySpark shiftRight). col >> n.
pub fn shift_right(column: &Column, n: i32) -> Column {
    column.clone().shift_right(n)
}

/// Bitwise unsigned right shift (PySpark shiftRightUnsigned). Logical shift for Long.
pub fn shift_right_unsigned(column: &Column, n: i32) -> Column {
    column.clone().shift_right_unsigned(n)
}

/// Session/library version string (PySpark version).
pub fn version() -> Column {
    Column::from_expr(
        lit(concat!("robin-sparkless-", env!("CARGO_PKG_VERSION"))),
        None,
    )
}

/// Null-safe equality: true if both null or both equal (PySpark equal_null). Alias for eq_null_safe.
pub fn equal_null(left: &Column, right: &Column) -> Column {
    left.clone().eq_null_safe(right)
}

/// Length of JSON array at path (PySpark json_array_length).
pub fn json_array_length(column: &Column, path: &str) -> Column {
    column.clone().json_array_length(path)
}

/// Parse URL and extract part: PROTOCOL, HOST, PATH, etc. (PySpark parse_url).
/// When key is Some(k) and part is QUERY/QUERYSTRING, returns the value for that query parameter only.
pub fn parse_url(column: &Column, part: &str, key: Option<&str>) -> Column {
    column.clone().parse_url(part, key)
}

/// Hash of column values (PySpark hash). Uses Murmur3 32-bit for parity with PySpark.
pub fn hash(columns: &[&Column]) -> Column {
    use polars::prelude::*;
    if columns.is_empty() {
        return crate::column::Column::from_expr(lit(0i64), None);
    }
    if columns.len() == 1 {
        return columns[0].clone().hash();
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let struct_expr = polars::prelude::as_struct(exprs);
    let name = columns[0].name().to_string();
    let expr = struct_expr.map(
        crate::udfs::apply_hash_struct,
        GetOutput::from_type(DataType::Int64),
    );
    crate::column::Column::from_expr(expr, Some(name))
}

/// Stack columns into struct (PySpark stack). Alias for struct_.
pub fn stack(columns: &[&Column]) -> Column {
    struct_(columns)
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
        let column = lit_f64(std::f64::consts::PI);
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

    #[test]
    fn test_cast_double_string_column_strict_ok() {
        // All values parse as doubles, so strict cast should succeed.
        let df = df!(
            "s" => &["123", " 45.5 ", "0"]
        )
        .unwrap();

        let s_col = col("s");
        let cast_col = cast(&s_col, "double").unwrap();

        let out = df
            .lazy()
            .with_column(cast_col.into_expr().alias("v"))
            .collect()
            .unwrap();

        let v = out.column("v").unwrap();
        let vals: Vec<Option<f64>> = v.f64().unwrap().into_iter().collect();
        assert_eq!(vals, vec![Some(123.0), Some(45.5), Some(0.0)]);
    }

    #[test]
    fn test_try_cast_double_string_column_invalid_to_null() {
        // Invalid numeric strings should become null under try_cast / try_to_number.
        let df = df!(
            "s" => &["123", " 45.5 ", "abc", ""]
        )
        .unwrap();

        let s_col = col("s");
        let try_cast_col = try_cast(&s_col, "double").unwrap();

        let out = df
            .lazy()
            .with_column(try_cast_col.into_expr().alias("v"))
            .collect()
            .unwrap();

        let v = out.column("v").unwrap();
        let vals: Vec<Option<f64>> = v.f64().unwrap().into_iter().collect();
        assert_eq!(vals, vec![Some(123.0), Some(45.5), None, None]);
    }

    #[test]
    fn test_to_number_and_try_to_number_numerics_and_strings() {
        // Mixed numeric types should be cast to double; invalid strings become null only for try_to_number.
        let df = df!(
            "i" => &[1i32, 2, 3],
            "f" => &[1.5f64, 2.5, 3.5],
            "s" => &["10", "20.5", "xyz"]
        )
        .unwrap();

        let i_col = col("i");
        let f_col = col("f");
        let s_col = col("s");

        let to_number_i = to_number(&i_col, None).unwrap();
        let to_number_f = to_number(&f_col, None).unwrap();
        let try_to_number_s = try_to_number(&s_col, None).unwrap();

        let out = df
            .lazy()
            .with_columns([
                to_number_i.into_expr().alias("i_num"),
                to_number_f.into_expr().alias("f_num"),
                try_to_number_s.into_expr().alias("s_num"),
            ])
            .collect()
            .unwrap();

        let i_num = out.column("i_num").unwrap();
        let f_num = out.column("f_num").unwrap();
        let s_num = out.column("s_num").unwrap();

        let i_vals: Vec<Option<f64>> = i_num.f64().unwrap().into_iter().collect();
        let f_vals: Vec<Option<f64>> = f_num.f64().unwrap().into_iter().collect();
        let s_vals: Vec<Option<f64>> = s_num.f64().unwrap().into_iter().collect();

        assert_eq!(i_vals, vec![Some(1.0), Some(2.0), Some(3.0)]);
        assert_eq!(f_vals, vec![Some(1.5), Some(2.5), Some(3.5)]);
        assert_eq!(s_vals, vec![Some(10.0), Some(20.5), None]);
    }
}
