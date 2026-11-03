//! Engine-neutral execution and storage capabilities.

mod cancellation;
mod collected;
mod context;
mod dataframe;
mod external_catalog;
mod join;
mod reader;
mod session;

pub use cancellation::CancellationToken;
pub use collected::CollectedRows;
pub use context::{ExecutionConfigSnapshot, ExecutionContext};
pub use dataframe::{FrameActions, FrameSchema, FrameTransform, GroupedData};
pub use external_catalog::{
    ArrowIpcBatchStream, ArrowIpcStream, CatalogBatchWriter, CatalogCapabilities, CatalogOwnership,
    CatalogReadSnapshot, CatalogReader, CatalogResolution, CatalogResolver, CatalogWriter,
    ExternalCatalog, ExternalTableDescriptor, ExternalTableFormat, ExternalTableSaveMode,
    ExternalTableWriteBatchRequest, ExternalTableWriteOutcome, ExternalTableWriteRequest,
    ExternalTableWriteResult, PreparedCatalogWrite, begin_catalog_read_snapshot,
    resolve_catalog_identifier,
};
pub use join::JoinType;
pub use reader::DataFrameReaderBackend;
pub use session::SparkSessionBackend;
