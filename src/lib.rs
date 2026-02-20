//! Robin Sparkless - A Rust DataFrame library with PySpark-like API
//!
//! This library provides a PySpark-compatible API. The **root crate** is engine-agnostic:
//! it depends on [robin-sparkless-core](https://docs.rs/robin-sparkless-core) (types, expression IR, config)
//! and one backend—currently **robin-sparkless-polars**, which uses [Polars](https://www.pola.rs/)
//! for execution. The public API exposes engine-agnostic expression types where possible.
//!
//! # Expression APIs
//!
//! - **ExprIr (engine-agnostic):** Use [`col`], [`lit_i64`], [`lit_str`], [`when`], [`gt`], [`eq`], etc.
//!   from the crate root (re-exported from `robin_sparkless_core`). These build an [`ExprIr`] tree.
//!   Use [`DataFrame::filter_expr_ir`], [`DataFrame::select_expr_ir`], [`DataFrame::with_column_expr_ir`],
//!   [`DataFrame::collect_rows`], and [`GroupedData::agg_expr_ir`] with `&ExprIr` / `&[ExprIr]`.
//!   Collect returns [`CollectedRows`] (JSON-like rows). Prefer this for new code and embeddings.
//!
//! - **Column / Expr (Polars-backed):** Use [`prelude`] or `robin_sparkless::functions::{col, lit_i64, ...}`
//!   for the full PySpark-like API that returns [`Column`] and uses Polars [`Expr`]. Use
//!   [`DataFrame::filter`], [`DataFrame::with_column`], [`DataFrame::select_exprs`], etc.
//!   with those types. Still supported for compatibility and advanced use.
//!
//! # Getting started and embedding
//!
//! For application code and embedding, use the [prelude]: `use robin_sparkless::prelude::*`.
//! For a minimal FFI surface, use [prelude::embed]. For engine-agnostic expressions, use the
//! root re-exports (`col`, `lit_i64`, `gt`, etc.) and the `*_expr_ir` / `collect_rows` methods.
//!
//! # Panics and errors
//!
//! Some functions panic when used with invalid or empty inputs (e.g. calling
//! `when(cond).otherwise(val)` without `.then()`, or passing no columns to
//! `format_string`, `elt`, `concat`, `coalesce`, or `named_struct` in Rust).
//! In Rust, `create_map` and `array` return `Result` for empty input instead of
//! panicking. From Python, empty columns for `coalesce`, `format_string`,
//! `printf`, and `named_struct` raise `ValueError`. See the documentation for
//! each function for details.
//!
//! # API stability
//!
//! While the crate is in the 0.x series, we follow [semver](https://semver.org/) but may introduce
//! breaking changes in minor releases (e.g. 0.1 → 0.2) until 1.0. For behavioral caveats and
//! intentional differences from PySpark, see the [repository documentation](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md).

#![allow(clippy::collapsible_if)]
#![allow(clippy::let_and_return)]

pub mod config;
pub mod dataframe;
pub mod prelude;
pub mod schema;
pub mod session;
pub mod traits;

// Re-export backend modules (column, functions, etc.) for internal use and backward compat.
pub use robin_sparkless_polars::{column, error, functions, type_coercion};

pub use robin_sparkless_polars::functions::{SortOrder, *};
/// Plan execution; use [`execute_plan`] for root session/DataFrame. Re-exports plan error types.
pub mod plan {
    pub use crate::execute_plan;
    pub use robin_sparkless_polars::plan::{PlanError, PlanExprError};
}
// Engine-agnostic types and expression IR from core (no Polars in public API).
pub use robin_sparkless_core::engine::CollectedRows;
pub use robin_sparkless_core::expr::{
    ExprIr, LiteralValue, WhenBuilder, WhenThenBuilder, alias, and_, between, call, col, count, eq,
    ge, gt, is_in, is_null, le, lit_bool, lit_f64, lit_i32, lit_i64, lit_null, lit_str, lt, max,
    mean, min, ne, not_, or_, sum, when,
};
pub use robin_sparkless_core::{DataType, EngineError, StructField, StructType};
pub use robin_sparkless_polars::{
    Column, Expr, PolarsError, RustUdf, StructTypePolarsExt, UdfRegistry, broadcast, expression,
    schema_from_json,
};

// Root-owned entry-point types (delegate to robin-sparkless-polars).
pub use dataframe::{
    CubeRollupData, DataFrame, DataFrameNa, DataFrameStat, DataFrameWriter, GroupedData, JoinType,
    PivotedGroupedData, SaveMode, SelectItem, WriteFormat, WriteMode,
};
pub use session::{DataFrameReader, SparkSession, SparkSessionBuilder};

// Root-owned traits (work with root DataFrame/SparkSession); plan re-export.
pub use robin_sparkless_polars::plan::{PlanError, PlanExprError};
pub use traits::{FromRobinDf, IntoRobinDf};

/// Execute a logical plan; returns root-owned [`DataFrame`].
pub fn execute_plan(
    session: &SparkSession,
    data: Vec<Vec<serde_json::Value>>,
    schema: Vec<(String, String)>,
    plan: &[serde_json::Value],
) -> Result<DataFrame, PlanError> {
    robin_sparkless_polars::plan::execute_plan(&session.0, data, schema, plan).map(DataFrame)
}

pub use config::SparklessConfig;

/// Convert PolarsError to EngineError (for APIs that still return PolarsError).
pub fn to_engine_error(e: PolarsError) -> EngineError {
    robin_sparkless_core::EngineError::from(robin_sparkless_polars::EngineError::from(e))
}

#[cfg(feature = "sql")]
pub mod sql {
    //! SQL parsing and execution; returns root-owned DataFrame.
    use crate::dataframe::DataFrame;
    use crate::session::SparkSession;
    use robin_sparkless_polars::PolarsError;

    pub use robin_sparkless_polars::sql::{Statement, execute_sql, parse_sql};

    /// Execute SQL and return root-owned DataFrame.
    pub fn execute_sql_root(session: &SparkSession, query: &str) -> Result<DataFrame, PolarsError> {
        robin_sparkless_polars::sql::execute_sql(&session.0, query).map(DataFrame)
    }
}

#[cfg(feature = "delta")]
pub mod delta {
    //! Delta Lake read/write; returns root-owned DataFrame where applicable.
    use crate::dataframe::DataFrame;
    use robin_sparkless_polars::PolarsError;
    use std::path::Path;

    pub use robin_sparkless_polars::delta::{read_delta, read_delta_with_version, write_delta};

    /// Read Delta table; returns root-owned DataFrame.
    pub fn read_delta_root(
        path: impl AsRef<Path>,
        case_sensitive: bool,
    ) -> Result<DataFrame, PolarsError> {
        robin_sparkless_polars::delta::read_delta(path, case_sensitive).map(DataFrame)
    }

    /// Read Delta table at version; returns root-owned DataFrame.
    pub fn read_delta_with_version_root(
        path: impl AsRef<Path>,
        version: Option<i64>,
        case_sensitive: bool,
    ) -> Result<DataFrame, PolarsError> {
        robin_sparkless_polars::delta::read_delta_with_version(path, version, case_sensitive)
            .map(DataFrame)
    }
}
