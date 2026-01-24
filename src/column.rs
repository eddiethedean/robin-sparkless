use pyo3::prelude::*;
use datafusion::prelude::*;
use datafusion::logical_expr::{Expr, Operator};
use crate::expression::{pyany_to_expr, build_comparison, build_arithmetic, python_to_literal};

#[pyclass]
#[derive(Debug, Clone)]
pub struct Column {
    #[pyo3(get)]
    name: String,
    expr: Expr, // DataFusion expression for lazy evaluation
}

#[pymethods]
impl Column {
    #[new]
    fn new(name: String) -> Self {
        Column {
            name: name.clone(),
            expr: datafusion::prelude::col(&name),
        }
    }
    
    fn __str__(&self) -> String {
        self.name.clone()
    }
    
    fn __repr__(&self) -> String {
        format!("Column('{}')", self.name)
    }
    
    // Comparison operators
    fn __lt__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_comparison(self.expr.clone(), "<", other_expr)?;
        Ok(Column {
            name: format!("({} < {})", self.name, other_display),
            expr,
        })
    }
    
    fn __le__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_comparison(self.expr.clone(), "<=", other_expr)?;
        Ok(Column {
            name: format!("({} <= {})", self.name, other_display),
            expr,
        })
    }
    
    fn __gt__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_comparison(self.expr.clone(), ">", other_expr)?;
        Ok(Column {
            name: format!("({} > {})", self.name, other_display),
            expr,
        })
    }
    
    fn __ge__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_comparison(self.expr.clone(), ">=", other_expr)?;
        Ok(Column {
            name: format!("({} >= {})", self.name, other_display),
            expr,
        })
    }
    
    fn __eq__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_comparison(self.expr.clone(), "=", other_expr)?;
        Ok(Column {
            name: format!("({} = {})", self.name, other_display),
            expr,
        })
    }
    
    fn __ne__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_comparison(self.expr.clone(), "!=", other_expr)?;
        Ok(Column {
            name: format!("({} != {})", self.name, other_display),
            expr,
        })
    }
    
    // Arithmetic operators
    fn __add__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_arithmetic(self.expr.clone(), "+", other_expr)?;
        Ok(Column {
            name: format!("({} + {})", self.name, other_display),
            expr,
        })
    }
    
    fn __sub__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_arithmetic(self.expr.clone(), "-", other_expr)?;
        Ok(Column {
            name: format!("({} - {})", self.name, other_display),
            expr,
        })
    }
    
    fn __mul__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_arithmetic(self.expr.clone(), "*", other_expr)?;
        Ok(Column {
            name: format!("({} * {})", self.name, other_display),
            expr,
        })
    }
    
    fn __truediv__(&self, other: &PyAny) -> PyResult<Column> {
        let other_expr = pyany_to_expr(other, None)?;
        let other_display = get_expr_display_name(&other_expr);
        let expr = build_arithmetic(self.expr.clone(), "/", other_expr)?;
        Ok(Column {
            name: format!("({} / {})", self.name, other_display),
            expr,
        })
    }
    
    // Column methods
    fn alias(&self, name: &str) -> Column {
        Column {
            name: name.to_string(),
            expr: self.expr.clone().alias(name),
        }
    }
    
    fn isNull(&self) -> Column {
        let expr = self.expr.clone().is_null();
        Column {
            name: format!("({} IS NULL)", self.name),
            expr,
        }
    }
    
    fn isNotNull(&self) -> Column {
        let expr = self.expr.clone().is_not_null();
        Column {
            name: format!("({} IS NOT NULL)", self.name),
            expr,
        }
    }
    
    fn like(&self, pattern: &str) -> PyResult<Column> {
        let pattern_expr = datafusion::prelude::lit(pattern);
        let expr = self.expr.clone().like(pattern_expr);
        Ok(Column {
            name: format!("({} LIKE '{}')", self.name, pattern),
            expr,
        })
    }
}

impl Column {
    /// Create a new Column from a name (internal use)
    pub fn from_name(name: String) -> Self {
        Column {
            name: name.clone(),
            expr: datafusion::prelude::col(&name),
        }
    }
    
    pub fn from_expr(expr: Expr, name: Option<String>) -> Self {
        let display_name = name.unwrap_or_else(|| get_expr_display_name(&expr));
        Column {
            name: display_name,
            expr,
        }
    }
    
    pub fn expr(&self) -> &Expr {
        &self.expr
    }
    
    pub fn to_expr(&self) -> PyResult<Expr> {
        Ok(self.expr.clone())
    }
}

/// Get a display name for an Expr (for debugging/display purposes)
fn get_expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(col) => col.name.clone(),
        Expr::Literal(lit) => format!("{:?}", lit),
        Expr::BinaryExpr(binary_expr) => {
            format!("({} {:?} {})", 
                get_expr_display_name(&binary_expr.left), 
                binary_expr.op, 
                get_expr_display_name(&binary_expr.right))
        }
        Expr::Alias(alias_expr) => {
            // Alias has expr and name fields
            format!("{}", alias_expr.name)
        }
        _ => format!("{:?}", expr),
    }
}
