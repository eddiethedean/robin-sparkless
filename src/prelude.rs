//! One-stop prelude for application code and embedding.
//!
//! Use `use robin_sparkless::prelude::*` to get the most common types and functions.
//! For the full API, see the crate root and [`crate::functions`].
//! For a minimal FFI/embedding surface, use [`crate::prelude::embed`].

pub mod embed;

pub use crate::column::Column;
pub use crate::config::SparklessConfig;
pub use crate::dataframe::{
    CubeRollupData, DataFrame, GroupedData, JoinType, PivotedGroupedData, SaveMode, WriteFormat,
    WriteMode,
};
pub use crate::functions::{
    SortOrder, asc, asc_nulls_first, asc_nulls_last, avg, coalesce, col, count, count_distinct,
    desc, desc_nulls_first, desc_nulls_last, first, length, lit_bool, lit_f64, lit_i32, lit_i64,
    lit_str, lower, max, mean, min, substring, sum, trim, upper, when,
};
pub use crate::schema::{DataType, StructField, StructType, StructTypePolarsExt};
pub use crate::session::{DataFrameReader, SparkSession, SparkSessionBuilder};
pub use crate::{Expr, LiteralValue};
