//! Minimal, stable surface for FFI and embedding crates.
//!
//! This module is the recommended import for bindings (e.g. PyO3, Node) that want
//! to depend only on robin-sparkless types. It is kept small and stable.

pub use crate::column::Column;
pub use crate::dataframe::{DataFrame, GroupedData};
pub use crate::functions::{avg, col, count, lit_bool, lit_i64, lit_null, lit_str, max, min, sum};
pub use crate::session::{SparkSession, SparkSessionBuilder};
pub use crate::{Expr, LiteralValue};
