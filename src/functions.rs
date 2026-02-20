//! Re-export expression functions from robin-sparkless-expr, plus broadcast (main-crate only).

pub use robin_sparkless_expr::functions::*;

use crate::dataframe::DataFrame;

/// Broadcast hint - no-op that returns the same DataFrame (PySpark broadcast).
pub fn broadcast(df: &DataFrame) -> DataFrame {
    df.clone()
}
