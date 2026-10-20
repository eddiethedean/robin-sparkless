//! Engine-neutral descriptors and lookup contract for external catalog tables.

use crate::EngineError;
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
}
