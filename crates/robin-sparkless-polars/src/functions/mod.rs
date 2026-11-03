//! Expression builders grouped by semantic category.
//! Public items are re-exported here to preserve the existing Rust API.

mod imports {
    pub(super) use crate::column::Column;
    pub(super) use num_bigint::BigInt;
    pub(super) use num_traits::{Signed, ToPrimitive, Zero};
    pub(super) use polars::prelude::*;
    pub(super) use std::collections::VecDeque;
}

mod sort;
pub use sort::*;
mod types;
pub use types::*;
mod column;
pub use column::*;

mod aggregations;
mod collections;
mod conditional;
mod datetime;
mod misc;
mod numeric;
mod strings;

pub use aggregations::*;
pub(crate) use aggregations::{DecimalPrefixExtrema, bind_runtime_config};
pub use collections::*;
pub use conditional::*;
pub use datetime::*;
pub use misc::*;
pub use numeric::*;
pub use strings::*;

#[cfg(test)]
mod tests;
