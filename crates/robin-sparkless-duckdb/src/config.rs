use robin_sparkless_core::EngineError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Trusted server configuration for a read-only standalone DuckDB database alias.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbSourceConfig {
    /// Prefix clients use in table names, such as `warehouse` in `warehouse.main.events`.
    pub alias: String,
    /// Path to an existing DuckDB database file. It is opened read-only.
    pub database_path: PathBuf,
}

/// Trusted server configuration for a read-only Quack-backed DuckDB alias.
///
/// `extension_path` may point to a local Quack extension built for the exact DuckDB version and
/// platform bundled with this Sparkless build. If omitted, the server looks for the extension
/// packaged beside its executable. It never downloads executable code at runtime.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbQuackSourceConfig {
    /// Prefix clients use in table names, such as `warehouse` in `warehouse.main.events`.
    pub alias: String,
    /// Quack URI such as `quack:localhost:9494` or `quack://localhost:9494`.
    pub quack_uri: String,
    /// Quack authentication token supplied by the trusted deployment configuration.
    pub token: String,
    /// Exact local DuckDB 2.0.0 Quack extension binary to load. Omit to use the extension shipped
    /// beside the Sparkless server executable.
    #[serde(default)]
    pub extension_path: PathBuf,
    /// Optional transport override. Omit to use Quack's local/remote defaults.
    #[serde(default)]
    pub disable_ssl: Option<bool>,
    /// Optional expected TLS certificate fingerprint for a self-signed remote certificate.
    #[serde(default)]
    pub ssl_fingerprint: Option<String>,
}

impl std::fmt::Debug for DuckDbQuackSourceConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DuckDbQuackSourceConfig")
            .field("alias", &self.alias)
            .field("quack_uri", &self.quack_uri)
            .field("token", &"[redacted]")
            .field("extension_path", &self.extension_path)
            .field("disable_ssl", &self.disable_ssl)
            .field(
                "ssl_fingerprint",
                &self.ssl_fingerprint.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

/// One trusted source definition accepted by [`DuckDbCatalog::new_with_sources`].
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DuckDbSourceSpec {
    /// Standalone read-only file mode.
    Local(DuckDbSourceConfig),
    /// Shared-owner Quack mode.
    Quack(DuckDbQuackSourceConfig),
}

#[derive(Clone)]
pub(crate) enum ConfiguredOwner {
    Local {
        database_path: PathBuf,
    },
    Quack {
        uri: String,
        token: String,
        extension_path: PathBuf,
        disable_ssl: Option<bool>,
        ssl_fingerprint: Option<String>,
    },
}

impl ConfiguredOwner {
    pub(crate) fn identity(&self) -> String {
        match self {
            Self::Local { database_path } => database_path.to_string_lossy().into_owned(),
            Self::Quack { uri, .. } => uri.clone(),
        }
    }

    pub(crate) fn quack_endpoint(&self) -> Option<&str> {
        match self {
            Self::Quack { uri, .. } => Some(uri),
            Self::Local { .. } => None,
        }
    }

    pub(crate) fn same_quack_connection(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Quack {
                    uri: left_uri,
                    token: left_token,
                    extension_path: left_extension,
                    disable_ssl: left_disable_ssl,
                    ssl_fingerprint: left_fingerprint,
                },
                Self::Quack {
                    uri: right_uri,
                    token: right_token,
                    extension_path: right_extension,
                    disable_ssl: right_disable_ssl,
                    ssl_fingerprint: right_fingerprint,
                },
            ) => {
                left_uri == right_uri
                    && left_token == right_token
                    && left_extension == right_extension
                    && left_disable_ssl == right_disable_ssl
                    && left_fingerprint == right_fingerprint
            }
            _ => false,
        }
    }
}

pub(crate) const QUACK_CATALOG: &str = "__sparkless_quack_source";

#[derive(Clone)]
pub(crate) struct ConfiguredSource {
    pub(crate) alias: String,
    pub(crate) owner: ConfiguredOwner,
}

pub(crate) fn validate_quack_uri(uri: &str) -> Result<(), EngineError> {
    let address = uri
        .strip_prefix("quack://")
        .or_else(|| uri.strip_prefix("quack:"));
    let Some(address) = address else {
        return Err(EngineError::invalid_input(
            "DuckDB Quack source URI must use the quack: scheme".into(),
        ));
    };
    let invalid = address.is_empty()
        || address.chars().any(|character| {
            character.is_whitespace()
                || matches!(character, '\'' | '"' | '@' | '/' | '?' | '#' | '\\' | '\0')
        });
    if invalid {
        return Err(EngineError::invalid_input(
            "DuckDB Quack source URI must contain only a host and optional port".into(),
        ));
    }
    Ok(())
}

pub(crate) fn resolve_quack_extension_path(
    configured_path: &Path,
) -> Result<PathBuf, std::io::Error> {
    let path = if configured_path.as_os_str().is_empty() {
        std::env::current_exe()?
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "server executable has no parent directory",
                )
            })?
            .join("quack.duckdb_extension")
    } else {
        configured_path.to_path_buf()
    };
    std::fs::canonicalize(path)
}

pub(crate) fn valid_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
