//! Conditional expression builders.

use super::imports::*;
use super::numeric::expr_coerce_to_boolean;

/// PySpark-style conditional expression builder.
///
/// # Example
/// ```
/// use robin_sparkless_polars::functions::{col, lit_i64, lit_str, when};
///
/// // when(condition).then(value).otherwise(fallback)
/// let expr = when(&col("age").gt(lit_i64(18).into_expr()))
///     .then(&lit_str("adult"))
///     .otherwise(&lit_str("minor"));
/// ```
/// Condition must be Boolean; then/else can be any type (#680). String conditions coerced via expr_coerce_to_boolean.
pub fn when(condition: &Column) -> WhenBuilder {
    WhenBuilder::new(expr_coerce_to_boolean(condition.expr().clone()))
}

/// Two-arg when(condition, value): returns value where condition is true, null otherwise (PySpark when(cond, val)).
/// Condition must be Boolean; value can be any type (#680). String conditions coerced via expr_coerce_to_boolean.
pub fn when_then_otherwise_null(condition: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let null_expr = lit(NULL);
    let expr = polars::prelude::when(expr_coerce_to_boolean(condition.expr().clone()))
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

    /// Specify the value when condition is true (condition already cast to Boolean in when()).
    pub fn then(self, value: &Column) -> ThenBuilder {
        use polars::prelude::*;
        let when_then = when(self.condition).then(value.expr().clone());
        ThenBuilder::new(when_then)
    }

    /// Specify the value when condition is false.
    /// Note: In PySpark, when(cond).otherwise(val) requires a .then() first.
    pub fn otherwise(self, _value: &Column) -> Result<Column, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "when().otherwise() requires .then() to be called first. Use when(cond).then(val1).otherwise(val2)"
                .into(),
        ))
    }
}

/// Builder for chaining when-then clauses before finalizing with otherwise
pub struct ThenBuilder {
    state: WhenThenState,
}

enum WhenThenState {
    Single(Box<polars::prelude::Then>),
    Chained(Box<polars::prelude::ChainedThen>),
}

/// Builder for an additional when-then clause (returned by ThenBuilder::when).
pub struct ChainedWhenBuilder {
    inner: polars::prelude::ChainedWhen,
}

impl ThenBuilder {
    fn new(when_then: polars::prelude::Then) -> Self {
        ThenBuilder {
            state: WhenThenState::Single(Box::new(when_then)),
        }
    }

    fn new_chained(chained: polars::prelude::ChainedThen) -> Self {
        ThenBuilder {
            state: WhenThenState::Chained(Box::new(chained)),
        }
    }

    /// Chain an additional when-then clause (PySpark: when(a).then(x).when(b).then(y).otherwise(z)).
    /// Condition must be Boolean (#680). String conditions coerced via expr_coerce_to_boolean.
    pub fn when(self, condition: &Column) -> ChainedWhenBuilder {
        let cond = expr_coerce_to_boolean(condition.expr().clone());
        let chained_when = match self.state {
            WhenThenState::Single(t) => t.when(cond),
            WhenThenState::Chained(ct) => ct.when(cond),
        };
        ChainedWhenBuilder {
            inner: chained_when,
        }
    }

    /// Finalize the expression with the fallback value
    pub fn otherwise(self, value: &Column) -> Column {
        let expr = match self.state {
            WhenThenState::Single(t) => t.otherwise(value.expr().clone()),
            WhenThenState::Chained(ct) => ct.otherwise(value.expr().clone()),
        };
        crate::column::Column::from_expr(expr, None)
    }
}

impl ChainedWhenBuilder {
    /// Set the value for the current when clause.
    pub fn then(self, value: &Column) -> ThenBuilder {
        ThenBuilder::new_chained(self.inner.then(value.expr().clone()))
    }
}
