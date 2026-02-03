use crate::column::Column;
use polars::prelude::*;

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
        _ => return Err(format!("unknown type name: {}", name)),
    })
}

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

/// Extract all matches of regex (PySpark regexp_extract_all).
pub fn regexp_extract_all(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_extract_all(pattern)
}

/// Check if string matches regex (PySpark regexp_like / rlike).
pub fn regexp_like(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_like(pattern)
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

/// Natural logarithm (PySpark log)
pub fn log(column: &Column) -> Column {
    column.clone().log()
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

/// Cast column to the given type (PySpark cast). Fails on invalid conversion.
pub fn cast(column: &Column, type_name: &str) -> Result<Column, String> {
    let dtype = parse_type_name(type_name)?;
    Ok(Column::from_expr(
        column.expr().clone().strict_cast(dtype),
        None,
    ))
}

/// Cast column to the given type, returning null on invalid conversion (PySpark try_cast).
pub fn try_cast(column: &Column, type_name: &str) -> Result<Column, String> {
    let dtype = parse_type_name(type_name)?;
    Ok(Column::from_expr(column.expr().clone().cast(dtype), None))
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

/// Format date/datetime as string (PySpark date_format). Uses chrono strftime format (e.g. "%Y-%m-%d").
pub fn date_format(column: &Column, format: &str) -> Column {
    column.clone().date_format(format)
}

/// Current date (evaluation time). PySpark current_date.
pub fn current_date() -> Column {
    use polars::prelude::*;
    let today = chrono::Utc::now().date_naive();
    let days = (today - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
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
pub fn months_between(end: &Column, start: &Column) -> Column {
    end.clone().months_between(start)
}

/// Next date that is the given weekday (e.g. "Mon") (PySpark next_day).
pub fn next_day(column: &Column, day_of_week: &str) -> Column {
    column.clone().next_day(day_of_week)
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

// --- Array / List functions (Phase 6a) ---

/// Create an array column from multiple columns (PySpark array).
pub fn array(columns: &[&Column]) -> crate::column::Column {
    use polars::prelude::*;
    if columns.is_empty() {
        panic!("array requires at least one column");
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let expr = concat_list(exprs).expect("concat_list");
    crate::column::Column::from_expr(expr, None)
}

/// Number of elements in list (PySpark size / array_size). Returns Int32.
pub fn array_size(column: &Column) -> Column {
    column.clone().array_size()
}

/// Alias for array_size (PySpark size).
pub fn size(column: &Column) -> Column {
    column.clone().array_size()
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

/// Slice list from 1-based start with optional length (PySpark slice).
pub fn array_slice(column: &Column, start: i64, length: Option<i64>) -> Column {
    column.clone().array_slice(start, length)
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

/// Mean of list elements (PySpark aggregate avg).
pub fn array_mean(column: &Column) -> Column {
    column.clone().array_mean()
}

/// Explode list with position (PySpark posexplode). Returns (pos_column, value_column).
/// pos is 1-based; implemented via list.eval(cum_count()).explode() and explode().
pub fn posexplode(column: &Column) -> (Column, Column) {
    column.clone().posexplode()
}

// --- Map functions (Phase 8) ---

/// Build a map column from alternating key/value expressions (PySpark create_map).
/// Returns List(Struct{key, value}) using Polars as_struct and concat_list.
pub fn create_map(key_values: &[&Column]) -> Column {
    use polars::prelude::{as_struct, concat_list};
    if key_values.is_empty() {
        panic!("create_map requires at least one key-value pair");
    }
    let mut struct_exprs: Vec<Expr> = Vec::new();
    for i in (0..key_values.len()).step_by(2) {
        if i + 1 < key_values.len() {
            let k = key_values[i].expr().clone().alias("key");
            let v = key_values[i + 1].expr().clone().alias("value");
            struct_exprs.push(as_struct(vec![k, v]));
        }
    }
    let expr = concat_list(struct_exprs).expect("create_map concat_list");
    crate::column::Column::from_expr(expr, None)
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

// --- JSON functions (Phase 10) ---

/// Extract JSON path from string column (PySpark get_json_object).
pub fn get_json_object(column: &Column, path: &str) -> Column {
    column.clone().get_json_object(path)
}

/// Parse string column as JSON into struct (PySpark from_json).
pub fn from_json(column: &Column, schema: Option<polars::datatypes::DataType>) -> Column {
    column.clone().from_json(schema)
}

/// Serialize struct column to JSON string (PySpark to_json).
pub fn to_json(column: &Column) -> Column {
    column.clone().to_json()
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
