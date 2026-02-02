use polars::prelude::{col, Expr};

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
