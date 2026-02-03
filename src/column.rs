use polars::prelude::{col, lit, Expr, ListNameSpaceExtension, RankMethod, RankOptions};

/// Column - represents a column in a DataFrame, used for building expressions
/// Thin wrapper around Polars `Expr`.
#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    expr: Expr, // Polars expression for lazy evaluation
}

impl Column {
    /// Create a new Column from a column name
    pub fn new(name: String) -> Self {
        Column {
            name: name.clone(),
            expr: col(&name),
        }
    }

    /// Create a Column from a Polars Expr
    pub fn from_expr(expr: Expr, name: Option<String>) -> Self {
        let display_name = name.unwrap_or_else(|| "<expr>".to_string());
        Column {
            name: display_name,
            expr,
        }
    }

    /// Get the underlying Polars Expr
    pub fn expr(&self) -> &Expr {
        &self.expr
    }

    /// Convert to Polars Expr (consumes self)
    pub fn into_expr(self) -> Expr {
        self.expr
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Alias the column
    pub fn alias(&self, name: &str) -> Column {
        Column {
            name: name.to_string(),
            expr: self.expr.clone().alias(name),
        }
    }

    /// Check if column is null
    pub fn is_null(&self) -> Column {
        Column {
            name: format!("({} IS NULL)", self.name),
            expr: self.expr.clone().is_null(),
        }
    }

    /// Check if column is not null
    pub fn is_not_null(&self) -> Column {
        Column {
            name: format!("({} IS NOT NULL)", self.name),
            expr: self.expr.clone().is_not_null(),
        }
    }

    /// Create a null boolean expression
    fn null_boolean_expr() -> Expr {
        use polars::prelude::*;
        // Create an expression that is always a null boolean
        lit(NULL).cast(DataType::Boolean)
    }

    /// Like pattern matching (substring search). Currently a no-op placeholder.
    pub fn like(&self, pattern: &str) -> Column {
        Column {
            name: format!("({} LIKE '{}')", self.name, pattern),
            // TODO: use Polars string contains when stabilized for this version
            expr: self.expr.clone(),
        }
    }

    /// PySpark-style equality comparison (NULL == NULL returns NULL, not True)
    /// Any comparison involving NULL returns NULL
    ///
    /// Explicitly wraps comparisons with null checks to ensure PySpark semantics.
    /// If either side is NULL, the result is NULL.
    pub fn eq_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard equality comparison
        let eq_result = self.expr().clone().eq(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(eq_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// PySpark-style inequality comparison (NULL != NULL returns NULL, not False)
    /// Any comparison involving NULL returns NULL
    pub fn ne_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard inequality comparison
        let ne_result = self.expr().clone().neq(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(ne_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// Null-safe equality (NULL <=> NULL returns True)
    /// PySpark's eqNullSafe() method
    pub fn eq_null_safe(&self, other: &Column) -> Column {
        use crate::functions::{lit_bool, when};

        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let both_null = left_null.clone().and(right_null.clone());
        let either_null = left_null.clone().or(right_null.clone());

        // Standard equality
        let eq_result = self.expr().clone().eq(other.expr().clone());

        // If both are null, return True
        // If either is null (but not both), return False
        // Otherwise, return standard equality result
        when(&Self::from_expr(both_null, None))
            .then(&lit_bool(true))
            .otherwise(
                &when(&Self::from_expr(either_null, None))
                    .then(&lit_bool(false))
                    .otherwise(&Self::from_expr(eq_result, None)),
            )
    }

    /// PySpark-style greater-than comparison (NULL > value returns NULL)
    /// Any comparison involving NULL returns NULL
    pub fn gt_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard greater-than comparison
        let gt_result = self.expr().clone().gt(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(gt_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// PySpark-style greater-than-or-equal comparison
    /// Any comparison involving NULL returns NULL
    pub fn ge_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard greater-than-or-equal comparison
        let ge_result = self.expr().clone().gt_eq(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(ge_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// PySpark-style less-than comparison
    /// Any comparison involving NULL returns NULL
    pub fn lt_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard less-than comparison
        let lt_result = self.expr().clone().lt(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(lt_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// PySpark-style less-than-or-equal comparison
    /// Any comparison involving NULL returns NULL
    pub fn le_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard less-than-or-equal comparison
        let le_result = self.expr().clone().lt_eq(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(le_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    // Standard comparison methods that work with Expr (for literals and columns)
    // These delegate to Polars and may not match PySpark null semantics exactly
    // Use _pyspark variants for explicit PySpark semantics

    /// Greater than comparison
    pub fn gt(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().gt(other), None)
    }

    /// Greater than or equal comparison
    pub fn gt_eq(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().gt_eq(other), None)
    }

    /// Less than comparison
    pub fn lt(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().lt(other), None)
    }

    /// Less than or equal comparison
    pub fn lt_eq(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().lt_eq(other), None)
    }

    /// Equality comparison
    pub fn eq(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().eq(other), None)
    }

    /// Inequality comparison
    pub fn neq(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().neq(other), None)
    }

    // --- String functions ---

    /// Convert string column to uppercase (PySpark upper)
    pub fn upper(&self) -> Column {
        Self::from_expr(self.expr().clone().str().to_uppercase(), None)
    }

    /// Convert string column to lowercase (PySpark lower)
    pub fn lower(&self) -> Column {
        Self::from_expr(self.expr().clone().str().to_lowercase(), None)
    }

    /// Substring with 1-based start (PySpark substring semantics)
    pub fn substr(&self, start: i64, length: Option<i64>) -> Column {
        use polars::prelude::*;
        let offset = (start - 1).max(0);
        let offset_expr = lit(offset);
        let length_expr = length.map(lit).unwrap_or_else(|| lit(i64::MAX)); // No length = rest of string
        Self::from_expr(
            self.expr().clone().str().slice(offset_expr, length_expr),
            None,
        )
    }

    /// String length in characters (PySpark length)
    pub fn length(&self) -> Column {
        Self::from_expr(self.expr().clone().str().len_chars(), None)
    }

    /// Trim leading and trailing whitespace (PySpark trim)
    pub fn trim(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(self.expr().clone().str().strip_chars(lit(" \t\n\r")), None)
    }

    /// Trim leading whitespace (PySpark ltrim)
    pub fn ltrim(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().strip_chars_start(lit(" \t\n\r")),
            None,
        )
    }

    /// Trim trailing whitespace (PySpark rtrim)
    pub fn rtrim(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().strip_chars_end(lit(" \t\n\r")),
            None,
        )
    }

    /// Extract first match of regex pattern (PySpark regexp_extract). Group 0 = full match.
    pub fn regexp_extract(&self, pattern: &str, group_index: usize) -> Column {
        use polars::prelude::*;
        let pat = pattern.to_string();
        Self::from_expr(
            self.expr().clone().str().extract(lit(pat), group_index),
            None,
        )
    }

    /// Replace first match of regex pattern (PySpark regexp_replace). literal=false for regex.
    pub fn regexp_replace(&self, pattern: &str, replacement: &str) -> Column {
        use polars::prelude::*;
        let pat = pattern.to_string();
        let rep = replacement.to_string();
        Self::from_expr(
            self.expr().clone().str().replace(lit(pat), lit(rep), false),
            None,
        )
    }

    /// Split string by delimiter (PySpark split). Returns list of strings.
    pub fn split(&self, delimiter: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().split(lit(delimiter.to_string())),
            None,
        )
    }

    /// Title case: first letter of each word uppercase (PySpark initcap).
    /// Approximates with lowercase when Polars to_titlecase is not enabled.
    pub fn initcap(&self) -> Column {
        Self::from_expr(self.expr().clone().str().to_lowercase(), None)
    }

    /// Extract all matches of regex (PySpark regexp_extract_all). Returns list of strings.
    pub fn regexp_extract_all(&self, pattern: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .str()
                .extract_all(lit(pattern.to_string())),
            None,
        )
    }

    /// Check if string matches regex (PySpark regexp_like / rlike).
    pub fn regexp_like(&self, pattern: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .str()
                .contains(lit(pattern.to_string()), false),
            None,
        )
    }

    // --- Math functions ---

    /// Absolute value (PySpark abs)
    pub fn abs(&self) -> Column {
        Self::from_expr(self.expr().clone().abs(), None)
    }

    /// Ceiling (PySpark ceil)
    pub fn ceil(&self) -> Column {
        Self::from_expr(self.expr().clone().ceil(), None)
    }

    /// Floor (PySpark floor)
    pub fn floor(&self) -> Column {
        Self::from_expr(self.expr().clone().floor(), None)
    }

    /// Round to given decimal places (PySpark round)
    pub fn round(&self, decimals: u32) -> Column {
        Self::from_expr(self.expr().clone().round(decimals), None)
    }

    // --- Datetime functions ---

    /// Extract year from datetime column (PySpark year)
    pub fn year(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().year(), None)
    }

    /// Extract month from datetime column (PySpark month)
    pub fn month(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().month(), None)
    }

    /// Extract day of month from datetime column (PySpark day)
    pub fn day(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().day(), None)
    }

    // --- Window functions ---

    /// Apply window partitioning. Returns a new Column with `.over(partition_by)`.
    /// Use after rank(), dense_rank(), row_number(), lag(), lead().
    pub fn over(&self, partition_by: &[&str]) -> Column {
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        Self::from_expr(self.expr().clone().over(partition_exprs), None)
    }

    /// Rank (with ties, gaps). Use with `.over(partition_by)`.
    pub fn rank(&self, descending: bool) -> Column {
        let opts = RankOptions {
            method: RankMethod::Min,
            descending,
        };
        Self::from_expr(self.expr().clone().rank(opts, None), None)
    }

    /// Dense rank (no gaps). Use with `.over(partition_by)`.
    pub fn dense_rank(&self, descending: bool) -> Column {
        let opts = RankOptions {
            method: RankMethod::Dense,
            descending,
        };
        Self::from_expr(self.expr().clone().rank(opts, None), None)
    }

    /// Row number (1, 2, 3 by this column's order). Use with `.over(partition_by)`.
    pub fn row_number(&self, descending: bool) -> Column {
        let opts = RankOptions {
            method: RankMethod::Ordinal,
            descending,
        };
        Self::from_expr(self.expr().clone().rank(opts, None), None)
    }

    /// Lag: value from n rows before. Use with `.over(partition_by)`.
    pub fn lag(&self, n: i64) -> Column {
        Self::from_expr(self.expr().clone().shift(polars::prelude::lit(n)), None)
    }

    /// Lead: value from n rows after. Use with `.over(partition_by)`.
    pub fn lead(&self, n: i64) -> Column {
        Self::from_expr(self.expr().clone().shift(polars::prelude::lit(-n)), None)
    }

    /// First value in partition (PySpark first_value). Use with `.over(partition_by)`.
    pub fn first_value(&self) -> Column {
        Self::from_expr(self.expr().clone().first(), None)
    }

    /// Last value in partition (PySpark last_value). Use with `.over(partition_by)`.
    pub fn last_value(&self) -> Column {
        Self::from_expr(self.expr().clone().last(), None)
    }

    /// Percent rank in partition: (rank - 1) / (count - 1). Window is applied; do not call .over() again.
    pub fn percent_rank(&self, partition_by: &[&str], descending: bool) -> Column {
        use polars::prelude::*;
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        let opts = RankOptions {
            method: RankMethod::Min,
            descending,
        };
        let rank_expr = self
            .expr()
            .clone()
            .rank(opts, None)
            .over(partition_exprs.clone());
        let count_expr = self.expr().clone().count().over(partition_exprs.clone());
        let rank_f = (rank_expr - lit(1i64)).cast(DataType::Float64);
        let count_f = (count_expr - lit(1i64)).cast(DataType::Float64);
        let pct = rank_f / count_f;
        Self::from_expr(pct, None)
    }

    /// Cumulative distribution in partition: row_number / count. Window is applied; do not call .over() again.
    pub fn cume_dist(&self, partition_by: &[&str], descending: bool) -> Column {
        use polars::prelude::*;
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        let opts = RankOptions {
            method: RankMethod::Ordinal,
            descending,
        };
        let row_num = self
            .expr()
            .clone()
            .rank(opts, None)
            .over(partition_exprs.clone());
        let count_expr = self.expr().clone().count().over(partition_exprs.clone());
        let cume = row_num / count_expr;
        Self::from_expr(cume.cast(DataType::Float64), None)
    }

    /// Ntile: bucket 1..n by rank within partition (ceil(rank * n / count)). Window is applied; do not call .over() again.
    pub fn ntile(&self, n: u32, partition_by: &[&str], descending: bool) -> Column {
        use polars::prelude::*;
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        let opts = RankOptions {
            method: RankMethod::Ordinal,
            descending,
        };
        let rank_expr = self
            .expr()
            .clone()
            .rank(opts, None)
            .over(partition_exprs.clone());
        let count_expr = self.expr().clone().count().over(partition_exprs.clone());
        let n_expr = lit(n as f64);
        let rank_f = rank_expr.cast(DataType::Float64);
        let count_f = count_expr.cast(DataType::Float64);
        let bucket = (rank_f * n_expr / count_f).ceil();
        let clamped = bucket.clip(lit(1.0), lit(n as f64));
        Self::from_expr(clamped.cast(DataType::Int32), None)
    }

    /// Nth value in partition by order (1-based n). Returns a Column with window already applied; do not call .over() again.
    pub fn nth_value(&self, n: i64, partition_by: &[&str], descending: bool) -> Column {
        use polars::prelude::*;
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        let opts = RankOptions {
            method: RankMethod::Ordinal,
            descending,
        };
        let rank_expr = self
            .expr()
            .clone()
            .rank(opts, None)
            .over(partition_exprs.clone());
        let cond_col = Self::from_expr(rank_expr.eq(lit(n)), None);
        let null_col = Self::from_expr(Expr::Literal(LiteralValue::Null), None);
        let value_col = Self::from_expr(self.expr().clone(), None);
        let when_expr = crate::functions::when(&cond_col)
            .then(&value_col)
            .otherwise(&null_col)
            .into_expr();
        let windowed = when_expr.max().over(partition_exprs);
        Self::from_expr(windowed, None)
    }

    // --- Array / List functions (Phase 6a) ---

    /// Number of elements in list (PySpark size / array_size). Returns Int32.
    pub fn array_size(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().list().len().cast(DataType::Int32),
            Some("size".to_string()),
        )
    }

    /// Check if list contains value (PySpark array_contains).
    pub fn array_contains(&self, value: Expr) -> Column {
        Self::from_expr(self.expr().clone().list().contains(value), None)
    }

    /// Join list of strings with separator (PySpark array_join).
    pub fn array_join(&self, separator: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .list()
                .join(lit(separator.to_string()), false),
            None,
        )
    }

    /// Maximum element in list (PySpark array_max).
    pub fn array_max(&self) -> Column {
        Self::from_expr(self.expr().clone().list().max(), None)
    }

    /// Minimum element in list (PySpark array_min).
    pub fn array_min(&self) -> Column {
        Self::from_expr(self.expr().clone().list().min(), None)
    }

    /// Get element at 1-based index (PySpark element_at). Returns null if out of bounds.
    pub fn element_at(&self, index: i64) -> Column {
        use polars::prelude::*;
        // PySpark uses 1-based indexing; Polars uses 0-based. index 1 -> get(0).
        let idx = if index >= 1 { index - 1 } else { index };
        Self::from_expr(self.expr().clone().list().get(lit(idx), true), None)
    }

    /// Sort list elements (PySpark array_sort). Ascending, nulls last.
    pub fn array_sort(&self) -> Column {
        use polars::prelude::SortOptions;
        let opts = SortOptions {
            descending: false,
            nulls_last: true,
            ..Default::default()
        };
        Self::from_expr(self.expr().clone().list().sort(opts), None)
    }

    /// Slice list from start with optional length (PySpark slice). 1-based start.
    pub fn array_slice(&self, start: i64, length: Option<i64>) -> Column {
        use polars::prelude::*;
        let start_expr = lit((start - 1).max(0)); // 1-based to 0-based
        let length_expr = length.map(lit).unwrap_or_else(|| lit(i64::MAX));
        Self::from_expr(
            self.expr().clone().list().slice(start_expr, length_expr),
            None,
        )
    }

    /// Explode list into one row per element (PySpark explode).
    pub fn explode(&self) -> Column {
        Self::from_expr(self.expr().clone().explode(), None)
    }

    /// 1-based index of first occurrence of value in list, or 0 if not found (PySpark array_position).
    /// Uses Polars list.eval with col("") as element (requires polars list_eval feature).
    pub fn array_position(&self, value: Expr) -> Column {
        use polars::prelude::{DataType, NULL};
        // In list.eval context, col("") refers to the current list element.
        let cond = Self::from_expr(col("").eq(value), None);
        let then_val = Self::from_expr(col("").cum_count(false), None);
        let else_val = Self::from_expr(lit(NULL), None);
        let idx_expr = crate::functions::when(&cond)
            .then(&then_val)
            .otherwise(&else_val)
            .into_expr();
        let list_expr = self
            .expr()
            .clone()
            .list()
            .eval(idx_expr, false)
            .list()
            .min()
            .fill_null(lit(0i64))
            .cast(DataType::Int64);
        Self::from_expr(list_expr, Some("array_position".to_string()))
    }

    /// New list with all elements equal to value removed (PySpark array_remove).
    /// Uses list.eval + drop_nulls (requires polars list_eval and list_drop_nulls).
    pub fn array_remove(&self, value: Expr) -> Column {
        use polars::prelude::NULL;
        // when(element != value) then element else null; then drop_nulls.
        let cond = Self::from_expr(col("").neq(value), None);
        let then_val = Self::from_expr(col(""), None);
        let else_val = Self::from_expr(lit(NULL), None);
        let elem_neq = crate::functions::when(&cond)
            .then(&then_val)
            .otherwise(&else_val)
            .into_expr();
        let list_expr = self
            .expr()
            .clone()
            .list()
            .eval(elem_neq, false)
            .list()
            .drop_nulls();
        Self::from_expr(list_expr, None)
    }

    /// Repeat each element n times (PySpark array_repeat). Not implemented: would require list.eval with dynamic repeat.
    pub fn array_repeat(&self, _n: i64) -> Column {
        unimplemented!(
            "array_repeat: not implemented (would require list.eval with repeat/flatten)"
        )
    }

    /// Explode list with position (PySpark posexplode). Returns (pos_col, value_col).
    /// pos is 1-based; uses list.eval(cum_count()).explode() and explode().
    pub fn posexplode(&self) -> (Column, Column) {
        let pos_expr = self
            .expr()
            .clone()
            .list()
            .eval(col("").cum_count(false), false)
            .explode();
        let val_expr = self.expr().clone().explode();
        (
            Self::from_expr(pos_expr, Some("pos".to_string())),
            Self::from_expr(val_expr, Some("col".to_string())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Column;
    use polars::prelude::{col, df, lit, IntoLazy};

    /// Helper to create a simple DataFrame for testing
    fn test_df() -> polars::prelude::DataFrame {
        df!(
            "a" => &[1, 2, 3, 4, 5],
            "b" => &[10, 20, 30, 40, 50]
        )
        .unwrap()
    }

    /// Helper to create a DataFrame with nulls for testing
    fn test_df_with_nulls() -> polars::prelude::DataFrame {
        df!(
            "a" => &[Some(1), Some(2), None, Some(4), None],
            "b" => &[Some(10), None, Some(30), None, None]
        )
        .unwrap()
    }

    #[test]
    fn test_column_new() {
        let column = Column::new("age".to_string());
        assert_eq!(column.name(), "age");
    }

    #[test]
    fn test_column_from_expr() {
        let expr = col("test");
        let column = Column::from_expr(expr, Some("test".to_string()));
        assert_eq!(column.name(), "test");
    }

    #[test]
    fn test_column_from_expr_default_name() {
        let expr = col("test").gt(lit(5));
        let column = Column::from_expr(expr, None);
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_column_alias() {
        let column = Column::new("original".to_string());
        let aliased = column.alias("new_name");
        assert_eq!(aliased.name(), "new_name");
    }

    #[test]
    fn test_column_gt() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.gt(lit(3));

        // Apply the expression to filter the DataFrame
        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 2); // rows with a > 3: 4, 5
    }

    #[test]
    fn test_column_lt() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.lt(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 2); // rows with a < 3: 1, 2
    }

    #[test]
    fn test_column_eq() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.eq(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 1); // only row with a == 3
    }

    #[test]
    fn test_column_neq() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.neq(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 4); // rows with a != 3
    }

    #[test]
    fn test_column_gt_eq() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.gt_eq(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 3); // rows with a >= 3: 3, 4, 5
    }

    #[test]
    fn test_column_lt_eq() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.lt_eq(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 3); // rows with a <= 3: 1, 2, 3
    }

    #[test]
    fn test_column_is_null() {
        let df = test_df_with_nulls();
        let column = Column::new("a".to_string());
        let result = column.is_null();

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 2); // 2 null values in column 'a'
    }

    #[test]
    fn test_column_is_not_null() {
        let df = test_df_with_nulls();
        let column = Column::new("a".to_string());
        let result = column.is_not_null();

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 3); // 3 non-null values in column 'a'
    }

    #[test]
    fn test_eq_null_safe_both_null() {
        // Create a DataFrame where both columns have NULL at the same row
        let df = df!(
            "a" => &[Some(1), None, Some(3)],
            "b" => &[Some(1), None, Some(4)]
        )
        .unwrap();

        let col_a = Column::new("a".to_string());
        let col_b = Column::new("b".to_string());
        let result = col_a.eq_null_safe(&col_b);

        // Apply the expression and collect
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("eq_null_safe"))
            .collect()
            .unwrap();

        // Get the result column
        let eq_col = result_df.column("eq_null_safe").unwrap();
        let values: Vec<Option<bool>> = eq_col.bool().unwrap().into_iter().collect();

        // Row 0: 1 == 1 -> true
        // Row 1: NULL <=> NULL -> true
        // Row 2: 3 == 4 -> false
        assert_eq!(values[0], Some(true));
        assert_eq!(values[1], Some(true)); // NULL-safe: both NULL = true
        assert_eq!(values[2], Some(false));
    }

    #[test]
    fn test_eq_null_safe_one_null() {
        // Create a DataFrame where only one column has NULL
        let df = df!(
            "a" => &[Some(1), None, Some(3)],
            "b" => &[Some(1), Some(2), None]
        )
        .unwrap();

        let col_a = Column::new("a".to_string());
        let col_b = Column::new("b".to_string());
        let result = col_a.eq_null_safe(&col_b);

        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("eq_null_safe"))
            .collect()
            .unwrap();

        let eq_col = result_df.column("eq_null_safe").unwrap();
        let values: Vec<Option<bool>> = eq_col.bool().unwrap().into_iter().collect();

        // Row 0: 1 == 1 -> true
        // Row 1: NULL <=> 2 -> false (one is null, not both)
        // Row 2: 3 <=> NULL -> false (one is null, not both)
        assert_eq!(values[0], Some(true));
        assert_eq!(values[1], Some(false));
        assert_eq!(values[2], Some(false));
    }
}
