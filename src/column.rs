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
        use crate::functions::{when, lit_bool};
        
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let both_null = left_null.clone().and(right_null.clone());
        let either_null = left_null.clone().or(right_null.clone());
        
        // Standard equality
        let eq_result = self.expr().clone().eq(other.expr().clone());
        
        // If both are null, return True
        // If either is null (but not both), return False
        // Otherwise, return standard equality result
        let null_safe_expr = when(&Self::from_expr(both_null, None))
            .then(&lit_bool(true))
            .otherwise(
                &when(&Self::from_expr(either_null, None))
                    .then(&lit_bool(false))
                    .otherwise(&Self::from_expr(eq_result, None))
            );
        
        null_safe_expr
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
