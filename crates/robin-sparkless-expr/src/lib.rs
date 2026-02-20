//! Robin Sparkless expr: Column, expressions, functions, type coercion, UDF registry and UDFs.

#![allow(clippy::collapsible_if)]
#![allow(clippy::let_and_return)]

pub mod column;
pub mod expression;
pub mod functions;
pub mod type_coercion;
pub mod udf_context;
pub mod udf_registry;
pub mod udfs;

pub use column::Column;
pub use expression::{column_to_expr, lit_bool, lit_f64, lit_i32, lit_i64, lit_str};
pub use functions::*;
pub use type_coercion::{
    CompareOp, coerce_for_pyspark_comparison, coerce_for_pyspark_eq_null_safe, find_common_type,
};
pub use udf_context::{get_thread_udf_context, set_thread_udf_context};
pub use udf_registry::{RustUdf, UdfRegistry};
