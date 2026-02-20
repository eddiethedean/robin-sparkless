//! One-stop prelude for application code and embedding.
//!
//! Use `use robin_sparkless::prelude::*` to get the most common types and functions.
//! The prelude provides the **Column-based API** (e.g. `col`, `lit_i64`, `when` from [`crate::functions`]),
//! which returns [`Column`] and works with [`DataFrame::filter`], [`DataFrame::with_column`], etc.
//!
//! For an **engine-agnostic API** (no Polars types in signatures), use the crate root:
//! `col`, `lit_i64`, `gt`, `when`, etc. build [`ExprIr`](crate::ExprIr), and use [`DataFrame::filter_expr_ir`],
//! [`DataFrame::collect_rows`], [`GroupedData::agg_expr_ir`], and the `*_engine()` methods.
//!
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
