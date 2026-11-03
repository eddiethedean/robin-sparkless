//! Element-wise UDF implementations grouped by semantic category.

use polars::prelude::PolarsError;

pub(crate) fn compute_err(context: &str, e: impl std::fmt::Display) -> PolarsError {
    PolarsError::ComputeError(format!("{}: {}", context, e).into())
}

use chrono::{Datelike, TimeZone};
use chrono_tz::Tz;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use polars::prelude::*;
use regex::Regex;
use std::borrow::Cow;
use std::ops::Not;

mod arithmetic;
mod arrays;
mod bitmap;
mod casts;
mod collections;
mod complex;
mod dates;
mod maps;
mod math;
mod strings;
mod temporal;
mod text;

pub use arithmetic::*;
pub use arrays::*;
pub use bitmap::*;
pub use casts::*;
pub use collections::*;
pub use complex::*;
pub use dates::*;
pub use maps::*;
pub use math::*;
pub use strings::*;
pub use temporal::*;
pub use text::*;

use arithmetic::unquote_simple_date_format;
use arrays::any_value_to_single_series;
use casts::parse_str_to_double;
use collections::{
    BITMAP_BYTES, cast_decimal_to_integer, parse_str_to_date, parse_str_to_datetime_micros,
    parse_str_to_int,
};
use complex::{
    binary_series_f64, binary_series_i32, binary_series_i64, broadcast_binary_inputs,
    pyspark_binary_arith, pyspark_decimal_binary_arith, series_to_f64_pyspark,
    try_narrow_integral_op,
};
use dates::{date_series_to_days, days_to_naive_date, naivedate_to_days};
use math::float_series_to_f64;
