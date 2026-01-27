use polars::prelude::{col, lit, Expr};

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

    /// Like pattern matching (substring search). Currently a no-op placeholder.
    pub fn like(&self, pattern: &str) -> Column {
        Column {
            name: format!("({} LIKE '{}')", self.name, pattern),
            // TODO: use Polars string contains when stabilized for this version
            expr: self.expr.clone(),
        }
    }
}
