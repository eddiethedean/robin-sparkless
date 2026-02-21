//! Element-wise UDFs for map() expressions (string, array, map, encoding, date, math, bit, etc.).

use polars::prelude::PolarsError;

pub(crate) fn compute_err(context: &str, e: impl std::fmt::Display) -> PolarsError {
    PolarsError::ComputeError(format!("{}: {}", context, e).into())
}

mod rest;
pub use rest::*;
