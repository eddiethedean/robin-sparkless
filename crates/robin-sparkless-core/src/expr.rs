//! Engine-agnostic expression IR. All backends interpret this; root and core only use ExprIr.

use serde::{Deserialize, Serialize};

/// Literal value in an expression (engine-agnostic).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiteralValue {
    I64(i64),
    F64(f64),
    I32(i32),
    Str(String),
    Bool(bool),
    Null,
}

/// Expression IR: a single, serializable tree that backends convert to their native Expr.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExprIr {
    /// Column reference: `col("name")`
    Column(String),
    /// Literal value
    Lit(LiteralValue),

    // --- Binary comparison (left, right) ---
    Eq(Box<ExprIr>, Box<ExprIr>),
    Ne(Box<ExprIr>, Box<ExprIr>),
    Gt(Box<ExprIr>, Box<ExprIr>),
    Ge(Box<ExprIr>, Box<ExprIr>),
    Lt(Box<ExprIr>, Box<ExprIr>),
    Le(Box<ExprIr>, Box<ExprIr>),
    EqNullSafe(Box<ExprIr>, Box<ExprIr>),

    // --- Logical ---
    And(Box<ExprIr>, Box<ExprIr>),
    Or(Box<ExprIr>, Box<ExprIr>),
    Not(Box<ExprIr>),

    // --- Arithmetic ---
    Add(Box<ExprIr>, Box<ExprIr>),
    Sub(Box<ExprIr>, Box<ExprIr>),
    Mul(Box<ExprIr>, Box<ExprIr>),
    Div(Box<ExprIr>, Box<ExprIr>),

    // --- Other binary ---
    Between {
        left: Box<ExprIr>,
        lower: Box<ExprIr>,
        upper: Box<ExprIr>,
    },
    IsIn(Box<ExprIr>, Box<ExprIr>),

    // --- Unary ---
    IsNull(Box<ExprIr>),
    IsNotNull(Box<ExprIr>),

    // --- Conditional ---
    When {
        condition: Box<ExprIr>,
        then_expr: Box<ExprIr>,
        otherwise: Box<ExprIr>,
    },

    /// Function call: name and args (e.g. sum, count, upper, substring, cast).
    Call {
        name: String,
        args: Vec<ExprIr>,
    },
}

// ---------- Builder helpers ----------

/// Column reference.
pub fn col(name: &str) -> ExprIr {
    ExprIr::Column(name.to_string())
}

pub fn lit_i64(n: i64) -> ExprIr {
    ExprIr::Lit(LiteralValue::I64(n))
}

pub fn lit_i32(n: i32) -> ExprIr {
    ExprIr::Lit(LiteralValue::I32(n))
}

pub fn lit_f64(n: f64) -> ExprIr {
    ExprIr::Lit(LiteralValue::F64(n))
}

pub fn lit_str(s: &str) -> ExprIr {
    ExprIr::Lit(LiteralValue::Str(s.to_string()))
}

pub fn lit_bool(b: bool) -> ExprIr {
    ExprIr::Lit(LiteralValue::Bool(b))
}

pub fn lit_null() -> ExprIr {
    ExprIr::Lit(LiteralValue::Null)
}

/// Generic function call (for the long tail of functions).
pub fn call(name: &str, args: Vec<ExprIr>) -> ExprIr {
    ExprIr::Call {
        name: name.to_string(),
        args,
    }
}

/// When-then-otherwise builder.
pub struct WhenBuilder {
    condition: ExprIr,
}

impl WhenBuilder {
    pub fn then(self, then_expr: ExprIr) -> WhenThenBuilder {
        WhenThenBuilder {
            condition: self.condition,
            then_expr,
        }
    }
}

pub struct WhenThenBuilder {
    condition: ExprIr,
    then_expr: ExprIr,
}

impl WhenThenBuilder {
    pub fn otherwise(self, otherwise: ExprIr) -> ExprIr {
        ExprIr::When {
            condition: Box::new(self.condition),
            then_expr: Box::new(self.then_expr),
            otherwise: Box::new(otherwise),
        }
    }
}

/// Start a when(condition).then(...).otherwise(...) chain.
pub fn when(condition: ExprIr) -> WhenBuilder {
    WhenBuilder {
        condition,
    }
}

// ---------- Common binary ops as ExprIr builders ----------

pub fn eq(a: ExprIr, b: ExprIr) -> ExprIr {
    ExprIr::Eq(Box::new(a), Box::new(b))
}

pub fn ne(a: ExprIr, b: ExprIr) -> ExprIr {
    ExprIr::Ne(Box::new(a), Box::new(b))
}

pub fn gt(a: ExprIr, b: ExprIr) -> ExprIr {
    ExprIr::Gt(Box::new(a), Box::new(b))
}

pub fn ge(a: ExprIr, b: ExprIr) -> ExprIr {
    ExprIr::Ge(Box::new(a), Box::new(b))
}

pub fn lt(a: ExprIr, b: ExprIr) -> ExprIr {
    ExprIr::Lt(Box::new(a), Box::new(b))
}

pub fn le(a: ExprIr, b: ExprIr) -> ExprIr {
    ExprIr::Le(Box::new(a), Box::new(b))
}

pub fn and_(a: ExprIr, b: ExprIr) -> ExprIr {
    ExprIr::And(Box::new(a), Box::new(b))
}

pub fn or_(a: ExprIr, b: ExprIr) -> ExprIr {
    ExprIr::Or(Box::new(a), Box::new(b))
}

pub fn not_(a: ExprIr) -> ExprIr {
    ExprIr::Not(Box::new(a))
}

pub fn is_null(a: ExprIr) -> ExprIr {
    ExprIr::IsNull(Box::new(a))
}

pub fn between(left: ExprIr, lower: ExprIr, upper: ExprIr) -> ExprIr {
    ExprIr::Between {
        left: Box::new(left),
        lower: Box::new(lower),
        upper: Box::new(upper),
    }
}

pub fn is_in(left: ExprIr, right: ExprIr) -> ExprIr {
    ExprIr::IsIn(Box::new(left), Box::new(right))
}

// ---------- Aggregation builders (ExprIr::Call) ----------

pub fn sum(expr: ExprIr) -> ExprIr {
    ExprIr::Call {
        name: "sum".to_string(),
        args: vec![expr],
    }
}

pub fn count(expr: ExprIr) -> ExprIr {
    ExprIr::Call {
        name: "count".to_string(),
        args: vec![expr],
    }
}

pub fn min(expr: ExprIr) -> ExprIr {
    ExprIr::Call {
        name: "min".to_string(),
        args: vec![expr],
    }
}

pub fn max(expr: ExprIr) -> ExprIr {
    ExprIr::Call {
        name: "max".to_string(),
        args: vec![expr],
    }
}

pub fn mean(expr: ExprIr) -> ExprIr {
    ExprIr::Call {
        name: "mean".to_string(),
        args: vec![expr],
    }
}

/// Alias an expression with a new output name.
pub fn alias(expr: ExprIr, name: &str) -> ExprIr {
    ExprIr::Call {
        name: "alias".to_string(),
        args: vec![expr, lit_str(name)],
    }
}
