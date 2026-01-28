use polars::prelude::*;
use crate::column::Column;

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
