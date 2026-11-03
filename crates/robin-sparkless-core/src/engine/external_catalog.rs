//! Owner-aware external catalog contracts with separate resolver, reader, and writer ports.

use crate::EngineError;
use crate::engine::CancellationToken;
use crate::schema::StructType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A storage format that a Sparkless backend can materialize for an external table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExternalTableFormat {
    Delta,
    DuckDb,
}

/// A stable catalog result that identifies a table snapshot independently of a catalog service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalTableDescriptor {
    pub table_id: String,
    pub canonical_location: String,
    pub schema: StructType,
    pub version: Option<i64>,
    pub provider: String,
    /// Opaque provider context; credentials and secrets must not be included.
    pub permissions_context: Option<String>,
    pub format: ExternalTableFormat,
}

/// Permissions advertised for a configured namespace owner, even if a requested table is absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCapabilities {
    pub read: bool,
    pub write: bool,
    pub batch_write: bool,
}

impl CatalogCapabilities {
    pub const READ_ONLY: Self = Self {
        read: true,
        write: false,
        batch_write: false,
    };
    pub const READ_WRITE: Self = Self {
        read: true,
        write: true,
        batch_write: false,
    };
    pub const READ_WRITE_BATCH: Self = Self {
        read: true,
        write: true,
        batch_write: true,
    };

    pub fn validate(self) -> Result<(), EngineError> {
        if self.batch_write && !self.write {
            return Err(EngineError::invalid_input(
                "batch-write capability requires write capability".into(),
            ));
        }
        Ok(())
    }
}

/// The owner for an identifier is resolved before looking up a table. `Local` permits the local
/// catalog to handle the identifier; `External` pins routing and forbids local fallthrough.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogOwnership {
    Local,
    External {
        owner_id: String,
        capabilities: CatalogCapabilities,
    },
}

impl CatalogOwnership {
    pub fn external(owner_id: impl Into<String>, capabilities: CatalogCapabilities) -> Self {
        Self::External {
            owner_id: owner_id.into(),
            capabilities,
        }
    }

    pub fn owner_id(&self) -> Option<&str> {
        match self {
            Self::Local => None,
            Self::External { owner_id, .. } => Some(owner_id),
        }
    }

    pub fn capabilities(&self) -> Option<CatalogCapabilities> {
        match self {
            Self::Local => None,
            Self::External { capabilities, .. } => Some(*capabilities),
        }
    }

    pub fn validate(&self) -> Result<(), EngineError> {
        if let Self::External {
            owner_id,
            capabilities,
        } = self
        {
            if owner_id.trim().is_empty() {
                return Err(EngineError::invalid_input(
                    "external catalog owner_id must not be empty".into(),
                ));
            }
            capabilities.validate()?;
        }
        Ok(())
    }
}

/// Ownership and metadata resolution are available without scanning rows or starting a write.
pub trait CatalogResolver: Send + Sync {
    /// Determine ownership before table existence. `Local` is the only result that permits
    /// fallback into Sparkless' local catalog.
    fn resolve_ownership(&self, identifier: &str) -> Result<CatalogOwnership, EngineError>;

    /// Resolve metadata after ownership is known. A missing table is `Ok(None)`; provider errors
    /// remain errors and must never be interpreted as a local-catalog miss.
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError>;
}

/// One statement's catalog view. Implementations may hold a transaction, lease, or connection
/// snapshot until the handle is dropped. Metadata lookup must not scan table rows.
pub trait CatalogReadSnapshot: Send + Sync {
    fn resolve_ownership(&self, identifier: &str) -> Result<CatalogOwnership, EngineError>;
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError>;
    fn scan_table(
        &self,
        table: &ExternalTableDescriptor,
        token: &CancellationToken,
        consume: &mut dyn FnMut(ArrowIpcStream) -> Result<(), EngineError>,
    ) -> Result<(), EngineError>;

    fn charge_decoded_bytes(&self, _bytes: usize) -> Result<(), EngineError> {
        Ok(())
    }
}

/// Optional read capability. Absence is represented by `None` on [`ExternalCatalog`].
pub trait CatalogReader: Send + Sync {
    fn begin_snapshot(
        &self,
        token: &CancellationToken,
    ) -> Result<Arc<dyn CatalogReadSnapshot>, EngineError>;
}

/// Materialized Arrow IPC streaming-format bytes passed to a native table writer.
#[derive(Debug, Clone)]
pub struct ArrowIpcStream {
    pub bytes: Vec<u8>,
    pub row_count: u64,
}

/// A back-pressured sequence of independently readable Arrow IPC batch streams.
pub struct ArrowIpcBatchStream {
    inner: Box<dyn Iterator<Item = Result<ArrowIpcStream, EngineError>> + Send>,
}

impl ArrowIpcBatchStream {
    pub fn new<I>(inner: I) -> Self
    where
        I: Iterator<Item = Result<ArrowIpcStream, EngineError>> + Send + 'static,
    {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl Iterator for ArrowIpcBatchStream {
    type Item = Result<ArrowIpcStream, EngineError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalTableSaveMode {
    ErrorIfExists,
    Overwrite,
    Append,
    Ignore,
}

#[derive(Debug, Clone)]
pub struct ExternalTableWriteRequest {
    pub operation_id: String,
    pub identifier: String,
    pub table: ExternalTableDescriptor,
    pub data: ArrowIpcStream,
    pub mode: ExternalTableSaveMode,
    pub format: Option<String>,
    pub options: Vec<(String, String)>,
    pub partition_by: Vec<String>,
}

pub struct ExternalTableWriteBatchRequest {
    pub operation_id: String,
    pub identifier: String,
    pub table: ExternalTableDescriptor,
    pub batches: ArrowIpcBatchStream,
    pub mode: ExternalTableSaveMode,
    pub format: Option<String>,
    pub options: Vec<(String, String)>,
    pub partition_by: Vec<String>,
}

/// Provider-neutral identity for a committed table write. Delta-style versions remain exact;
/// adapters with another commit model supply a provider-specific identity instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalTableWriteResult {
    pub table: ExternalTableDescriptor,
    pub committed_version: Option<i64>,
    pub provider_commit_id: Option<String>,
}

/// Write completion is explicit so a lost response is never mistaken for a rollback.
#[derive(Debug)]
pub enum ExternalTableWriteOutcome {
    Committed(ExternalTableWriteResult),
    NotCommitted(EngineError),
    OutcomeUnknown {
        provider: String,
        operation_id: String,
        message: String,
    },
}

/// A fully validated/staged write. `commit` is the point where the provider may accept an
/// irreversible operation; cancellation is checked by the catalog before this call.
pub trait PreparedCatalogWrite: Send {
    fn commit(self: Box<Self>, token: &CancellationToken) -> ExternalTableWriteOutcome;
}

/// Optional materialized-write capability. A read-only catalog has no writer object.
pub trait CatalogWriter: Send + Sync {
    fn prepare_write(
        &self,
        request: ExternalTableWriteRequest,
    ) -> Result<Box<dyn PreparedCatalogWrite>, EngineError>;
}

/// Optional bounded-batch write capability; independent from materialized writes.
pub trait CatalogBatchWriter: Send + Sync {
    fn prepare_write_batches(
        &self,
        request: ExternalTableWriteBatchRequest,
    ) -> Result<Box<dyn PreparedCatalogWrite>, EngineError>;
}

/// Composed provider capabilities for one logical session.
#[derive(Clone)]
pub struct ExternalCatalog {
    resolver: Arc<dyn CatalogResolver>,
    reader: Option<Arc<dyn CatalogReader>>,
    writer: Option<Arc<dyn CatalogWriter>>,
    batch_writer: Option<Arc<dyn CatalogBatchWriter>>,
}

impl std::fmt::Debug for ExternalCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalCatalog")
            .field("has_reader", &self.reader.is_some())
            .field("has_writer", &self.writer.is_some())
            .field("has_batch_writer", &self.batch_writer.is_some())
            .finish_non_exhaustive()
    }
}

impl ExternalCatalog {
    pub fn new(
        resolver: Arc<dyn CatalogResolver>,
        reader: Option<Arc<dyn CatalogReader>>,
        writer: Option<Arc<dyn CatalogWriter>>,
        batch_writer: Option<Arc<dyn CatalogBatchWriter>>,
    ) -> Self {
        Self {
            resolver,
            reader,
            writer,
            batch_writer,
        }
    }

    pub fn resolve_ownership(&self, identifier: &str) -> Result<CatalogOwnership, EngineError> {
        let ownership = self.resolver.resolve_ownership(identifier)?;
        ownership.validate()?;
        Ok(ownership)
    }

    pub fn resolve_table(
        &self,
        ownership: &CatalogOwnership,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        ownership.validate()?;
        match ownership {
            CatalogOwnership::Local => Ok(None),
            CatalogOwnership::External { .. } => {
                let table = self.resolver.resolve_table(identifier)?;
                if let Some(table) = &table {
                    table.validate()?;
                }
                Ok(table)
            }
        }
    }

    pub fn begin_snapshot(
        &self,
        ownership: &CatalogOwnership,
        token: &CancellationToken,
    ) -> Result<Arc<dyn CatalogReadSnapshot>, EngineError> {
        self.require_read_capability(ownership)?;
        let reader = self.reader.as_ref().ok_or_else(|| {
            EngineError::capability(
                "READ_CAPABILITY_MISSING",
                "external catalog has no read capability",
            )
        })?;
        reader.begin_snapshot(token)
    }

    pub fn require_read_capability(&self, ownership: &CatalogOwnership) -> Result<(), EngineError> {
        ownership.validate()?;
        let capabilities = ownership.capabilities().ok_or_else(|| {
            EngineError::invalid_input(
                "a local catalog namespace has no external read snapshot".into(),
            )
        })?;
        if !capabilities.read {
            return Err(EngineError::capability(
                "READ_CAPABILITY_DENIED",
                format!(
                    "external namespace '{}' does not support reads",
                    ownership.owner_id().unwrap_or_default()
                ),
            ));
        }
        Ok(())
    }

    pub fn has_materialized_writer(&self) -> bool {
        self.writer.is_some()
    }

    pub fn has_materialized_writer_for(&self, ownership: &CatalogOwnership) -> bool {
        ownership
            .capabilities()
            .is_some_and(|capabilities| capabilities.write)
            && self.writer.is_some()
    }

    pub fn has_reader(&self) -> bool {
        self.reader.is_some()
    }

    pub fn begin_operation_snapshot(
        &self,
        token: &CancellationToken,
    ) -> Result<Arc<dyn CatalogReadSnapshot>, EngineError> {
        token.check_cancelled()?;
        self.reader
            .as_ref()
            .ok_or_else(|| {
                EngineError::capability(
                    "READ_CAPABILITY_MISSING",
                    "external catalog has no read capability",
                )
            })?
            .begin_snapshot(token)
    }

    pub fn has_batch_writer(&self) -> bool {
        self.batch_writer.is_some()
    }

    pub fn has_batch_writer_for(&self, ownership: &CatalogOwnership) -> bool {
        ownership
            .capabilities()
            .is_some_and(|capabilities| capabilities.batch_write)
            && self.batch_writer.is_some()
    }

    pub fn write_table(
        &self,
        ownership: &CatalogOwnership,
        request: ExternalTableWriteRequest,
        token: &CancellationToken,
    ) -> Result<ExternalTableWriteOutcome, EngineError> {
        self.require_write_ownership(ownership)?;
        let writer = self.writer.as_ref().ok_or_else(|| {
            EngineError::capability(
                "MATERIALIZED_WRITE_CAPABILITY_MISSING",
                "external catalog has no materialized write capability",
            )
        })?;
        token.check_cancelled()?;
        let prepared = writer.prepare_write(request)?;
        token.check_cancelled()?;
        Ok(prepared.commit(token))
    }

    pub fn write_table_batches(
        &self,
        ownership: &CatalogOwnership,
        request: ExternalTableWriteBatchRequest,
        token: &CancellationToken,
    ) -> Result<ExternalTableWriteOutcome, EngineError> {
        self.require_write_ownership(ownership)?;
        if !ownership
            .capabilities()
            .is_some_and(|capabilities| capabilities.batch_write)
        {
            return Err(EngineError::capability(
                "BATCH_WRITE_CAPABILITY_DENIED",
                "external catalog owner does not permit batch writes",
            ));
        }
        let writer = self.batch_writer.as_ref().ok_or_else(|| {
            EngineError::capability(
                "BATCH_WRITE_CAPABILITY_MISSING",
                "external catalog has no batch-write capability",
            )
        })?;
        token.check_cancelled()?;
        let prepared = writer.prepare_write_batches(request)?;
        token.check_cancelled()?;
        Ok(prepared.commit(token))
    }

    fn require_write_ownership(&self, ownership: &CatalogOwnership) -> Result<(), EngineError> {
        ownership.validate()?;
        let Some(capabilities) = ownership.capabilities() else {
            return Err(EngineError::invalid_input(
                "cannot write an external table owned by the local catalog".into(),
            ));
        };
        if !capabilities.write {
            return Err(EngineError::capability(
                "WRITE_CAPABILITY_DENIED",
                format!(
                    "external namespace '{}' is read-only",
                    ownership.owner_id().unwrap_or_default()
                ),
            ));
        }
        if self.writer.is_none() && self.batch_writer.is_none() {
            return Err(EngineError::capability(
                "WRITE_CAPABILITY_MISSING",
                "external catalog owner advertises writes but no writer capability is registered",
            ));
        }
        Ok(())
    }
}

/// Resolve a table and its namespace owner without scanning. A missing table in an external
/// namespace remains externally owned and cannot be looked up in the local catalog.
#[derive(Debug, Clone)]
pub struct CatalogResolution {
    pub ownership: CatalogOwnership,
    pub table: Option<ExternalTableDescriptor>,
}

pub fn resolve_catalog_identifier(
    catalog: &ExternalCatalog,
    identifier: &str,
) -> Result<CatalogResolution, EngineError> {
    let ownership = catalog.resolve_ownership(identifier)?;
    let table = catalog.resolve_table(&ownership, identifier)?;
    Ok(CatalogResolution { ownership, table })
}

pub fn begin_catalog_read_snapshot(
    catalog: &ExternalCatalog,
    token: &CancellationToken,
) -> Result<Arc<dyn CatalogReadSnapshot>, EngineError> {
    catalog.begin_operation_snapshot(token)
}

impl ExternalTableWriteResult {
    pub fn validate_against(&self, previous: &ExternalTableDescriptor) -> Result<(), EngineError> {
        self.table.validate()?;
        if self.committed_version.is_none() && self.provider_commit_id.is_none() {
            return Err(EngineError::invalid_input(
                "external write receipt must include a version or provider commit identity".into(),
            ));
        }
        if self
            .committed_version
            .is_some_and(|version| version < 0 || self.table.version != Some(version))
        {
            return Err(EngineError::invalid_input(
                "external write receipt version must match its committed descriptor".into(),
            ));
        }
        if self
            .provider_commit_id
            .as_ref()
            .is_some_and(|identity| identity.trim().is_empty())
        {
            return Err(EngineError::invalid_input(
                "external write receipt provider identity must not be empty".into(),
            ));
        }
        if self.table.table_id != previous.table_id
            || self.table.canonical_location != previous.canonical_location
            || self.table.provider != previous.provider
            || self.table.format != previous.format
        {
            return Err(EngineError::invalid_input(
                "external table write result changed the resolved table identity".into(),
            ));
        }
        if self
            .committed_version
            .zip(previous.version)
            .is_some_and(|(committed, previous)| committed < previous)
        {
            return Err(EngineError::invalid_input(
                "external table write result moved the committed version backwards".into(),
            ));
        }
        Ok(())
    }
}

impl ExternalTableDescriptor {
    pub fn validate(&self) -> Result<(), EngineError> {
        if self.table_id.trim().is_empty() {
            return Err(EngineError::invalid_input(
                "external table descriptor table_id must not be empty".into(),
            ));
        }
        if self.canonical_location.trim().is_empty() {
            return Err(EngineError::invalid_input(
                "external table descriptor canonical_location must not be empty".into(),
            ));
        }
        if self.provider.trim().is_empty() {
            return Err(EngineError::invalid_input(
                "external table descriptor provider must not be empty".into(),
            ));
        }
        if self.version.is_some_and(|version| version < 0) {
            return Err(EngineError::invalid_input(
                "external table descriptor version must not be negative".into(),
            ));
        }
        Ok(())
    }
}
