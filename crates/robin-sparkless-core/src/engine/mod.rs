//! Engine traits: backend-agnostic session, reader, dataframe, grouped, and plan.
//!
//! This module defines the **engine-facing abstraction layer** used by the root crate
//! and by backend implementations such as `robin-sparkless-polars`:
//! - [`SparkSessionBackend`] creates readers, tables, and engine-owned DataFrames.
//! - [`DataFrameReaderBackend`] loads data from CSV/Parquet/JSON or existing tables.
//! - [`DataFrameBackend`] and [`GroupedDataBackend`] perform core DataFrame and grouped
//!   operations in terms of the engine-agnostic [`crate::expr::ExprIr`] expression IR.
//! - [`PlanExecutor`] executes JSON logical plans and returns boxed [`DataFrameBackend`]
//!   trait objects.
//!
//! Backends implement these traits (usually in their own `engine_backend` module) so that
//! high-level APIs in the root crate can depend on **traits and IR** instead of concrete
//! execution engines. This keeps core logic open for new backends without changing the
//! public engine interfaces.

mod collected;
mod dataframe;
mod join;
mod plan;
mod reader;
mod session;

pub use collected::CollectedRows;
pub use dataframe::{DataFrameBackend, GroupedDataBackend};
pub use join::JoinType;
pub use plan::PlanExecutor;
pub use reader::DataFrameReaderBackend;
pub use session::SparkSessionBackend;
