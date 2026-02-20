//! Engine traits: backend-agnostic session, reader, dataframe, grouped, plan.

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
