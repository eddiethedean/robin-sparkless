//! Engine-neutral descriptors and lookup contract for external catalog tables.

use crate::EngineError;
use crate::engine::CancellationToken;
use crate::schema::StructType;
use serde::{Deserialize, Serialize};

/// A storage format that a Sparkless backend can materialize for an external table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExternalTableFormat {
    /// A Delta Lake table readable by the selected Sparkless backend.
    Delta,
}

/// A stable catalog result that identifies a table snapshot independently of a catalog service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalTableDescriptor {
    /// Stable identity assigned by the owning catalog.
    pub table_id: String,
    /// Canonical storage location accepted by the backend's native reader.
    pub canonical_location: String,
    /// Schema declared by the owning catalog for this snapshot.
    pub schema: StructType,
    /// Requested snapshot version; `None` selects the latest committed version.
    pub version: Option<i64>,
    /// Name of the native provider that owns the table.
    pub provider: String,
    /// Optional opaque caller context. This must not contain credentials or secrets.
    pub permissions_context: Option<String>,
    /// Storage format used to materialize this table.
    pub format: ExternalTableFormat,
}

/// Materialized Arrow IPC streaming-format bytes passed to a native table writer.
///
/// The payload is an Apache Arrow IPC stream, not an IPC file. This representation is
/// engine-neutral so a catalog/table provider can consume typed values without decoding
/// JSON rows or depending on Polars types.
#[derive(Debug, Clone)]
pub struct ArrowIpcStream {
    pub bytes: Vec<u8>,
}

/// Save mode for a write to a table already resolved by an external catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalTableSaveMode {
    ErrorIfExists,
    Overwrite,
    Append,
    Ignore,
}

/// Typed data and the exact writer settings for one resolved external table.
#[derive(Debug, Clone)]
pub struct ExternalTableWriteRequest {
    /// Identifier supplied to `saveAsTable`; the descriptor provides stable target identity.
    pub identifier: String,
    pub table: ExternalTableDescriptor,
    pub data: ArrowIpcStream,
    pub mode: ExternalTableSaveMode,
    /// Format selected by the caller, if one was specified.
    pub format: Option<String>,
    /// Caller-specified writer options, preserving order and duplicate keys.
    pub options: Vec<(String, String)>,
    pub partition_by: Vec<String>,
}

/// The catalog state observed after a successful delegated write.
#[derive(Debug, Clone)]
pub struct ExternalTableWriteResult {
    /// Descriptor for the committed table snapshot.
    pub table: ExternalTableDescriptor,
    /// Exact version committed by the provider.
    pub committed_version: i64,
}

impl ExternalTableWriteResult {
    /// Validate that the receipt keeps the target identity stable and reports its commit.
    pub fn validate_against(&self, previous: &ExternalTableDescriptor) -> Result<(), EngineError> {
        self.table.validate()?;
        if self.committed_version < 0 {
            return Err(EngineError::User(
                "external table write result version must not be negative".into(),
            ));
        }
        if self.table.version != Some(self.committed_version) {
            return Err(EngineError::User(
                "external table write result descriptor must identify its committed version".into(),
            ));
        }
        if self.table.table_id != previous.table_id
            || self.table.canonical_location != previous.canonical_location
            || self.table.provider != previous.provider
            || self.table.format != previous.format
        {
            return Err(EngineError::User(
                "external table write result changed the resolved table identity".into(),
            ));
        }
        if previous
            .version
            .is_some_and(|version| self.committed_version < version)
        {
            return Err(EngineError::User(
                "external table write result moved the committed version backwards".into(),
            ));
        }
        Ok(())
    }
}

impl ExternalTableDescriptor {
    /// Reject incomplete or invalid identity/snapshot fields before storage access.
    pub fn validate(&self) -> Result<(), EngineError> {
        if self.table_id.trim().is_empty() {
            return Err(EngineError::User(
                "external table descriptor table_id must not be empty".into(),
            ));
        }
        if self.canonical_location.trim().is_empty() {
            return Err(EngineError::User(
                "external table descriptor canonical_location must not be empty".into(),
            ));
        }
        if self.provider.trim().is_empty() {
            return Err(EngineError::User(
                "external table descriptor provider must not be empty".into(),
            ));
        }
        if self.version.is_some_and(|version| version < 0) {
            return Err(EngineError::User(
                "external table descriptor version must not be negative".into(),
            ));
        }
        Ok(())
    }
}

/// Resolves an exact table identifier to metadata needed for native relation reads.
///
/// The identifier is passed as received by the Sparkless API; implementations own any
/// namespace parsing and case rules. A missing table is `Ok(None)`. Provider failures
/// must be returned as errors and are never treated as a missing table.
pub trait ExternalCatalogProvider: Send + Sync {
    /// Resolve `identifier` to the table's stable identity and committed snapshot.
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError>;

    /// Commit a write to a table previously resolved by this provider.
    ///
    /// Providers that are read-only keep the explicit default error. Returning a successful
    /// receipt means the provider has committed `data` and the descriptor identifies that
    /// exact version. Sparkless never falls back to a local write after a provider error.
    fn write_table(
        &self,
        _request: ExternalTableWriteRequest,
    ) -> Result<ExternalTableWriteResult, EngineError> {
        Err(EngineError::User(
            "external catalog provider does not support table writes".into(),
        ))
    }

    /// Commit a write while honoring cancellation before commit acceptance.
    ///
    /// Providers may override this method to add safe points before their commit. Returning
    /// [`EngineError::Cancelled`] promises that the provider did not accept the commit. Once
    /// the provider's commit path begins, a successful receipt or provider error is returned
    /// unchanged even if cancellation is requested concurrently; this keeps the caller's
    /// outcome truthful when commit state is known only to the provider.
    fn write_table_with_cancellation(
        &self,
        request: ExternalTableWriteRequest,
        token: &CancellationToken,
    ) -> Result<ExternalTableWriteResult, EngineError> {
        token.check_cancelled()?;
        self.write_table(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor() -> ExternalTableDescriptor {
        ExternalTableDescriptor {
            table_id: "table-123".into(),
            canonical_location: "/workspace/tables/table-123".into(),
            schema: StructType::new(Vec::new()),
            version: Some(0),
            provider: "delta-rs".into(),
            permissions_context: None,
            format: ExternalTableFormat::Delta,
        }
    }

    #[test]
    fn external_table_descriptor_requires_stable_identity_and_nonnegative_version() {
        let mut valid = descriptor();
        assert!(valid.validate().is_ok());

        valid.table_id.clear();
        assert!(
            matches!(valid.validate(), Err(EngineError::User(message)) if message.contains("table_id"))
        );

        let mut valid = descriptor();
        valid.canonical_location.clear();
        assert!(
            matches!(valid.validate(), Err(EngineError::User(message)) if message.contains("canonical_location"))
        );

        let mut valid = descriptor();
        valid.provider.clear();
        assert!(
            matches!(valid.validate(), Err(EngineError::User(message)) if message.contains("provider"))
        );

        let mut valid = descriptor();
        valid.version = Some(-1);
        assert!(
            matches!(valid.validate(), Err(EngineError::User(message)) if message.contains("version"))
        );
    }

    #[test]
    fn external_table_write_receipt_keeps_identity_and_reports_exact_monotonic_version() {
        let mut previous = descriptor();
        previous.version = Some(4);
        let mut table = previous.clone();
        table.version = Some(5);
        let valid = ExternalTableWriteResult {
            table: table.clone(),
            committed_version: 5,
        };
        assert!(valid.validate_against(&previous).is_ok());

        let mut changed_identity = valid.clone();
        changed_identity.table.table_id = "another-table".into();
        assert!(matches!(
            changed_identity.validate_against(&previous),
            Err(EngineError::User(message)) if message.contains("identity")
        ));

        let mut mismatched_version = valid.clone();
        mismatched_version.table.version = Some(6);
        assert!(matches!(
            mismatched_version.validate_against(&previous),
            Err(EngineError::User(message)) if message.contains("committed version")
        ));

        let mut backwards = valid;
        backwards.committed_version = 3;
        backwards.table.version = Some(3);
        assert!(matches!(
            backwards.validate_against(&previous),
            Err(EngineError::User(message)) if message.contains("backwards")
        ));
    }
}
