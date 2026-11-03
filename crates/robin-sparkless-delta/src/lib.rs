//! Delta storage-driver boundary, independent of the Polars execution engine.
//!
//! The Polars adapter owns Arrow-to-Polars conversion and query planning. This crate owns Delta
//! path normalization and snapshot loading so storage lifecycle decisions do not live in the
//! engine adapter.

use robin_sparkless_core::EngineError;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum DeltaLoadError {
    NotATable,
    Failure(EngineError),
}

impl std::fmt::Display for DeltaLoadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotATable => formatter.write_str("Not a Delta table"),
            Self::Failure(error) => write!(formatter, "{error}"),
        }
    }
}

#[cfg(feature = "driver")]
pub struct DeltaSnapshot {
    pub table: deltalake::DeltaTable,
    pub table_uri: String,
    runtime: tokio::runtime::Runtime,
}

#[cfg(feature = "driver")]
impl DeltaSnapshot {
    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// Transfer the loaded table and its runtime to a storage operation that needs mutable access.
    pub fn into_parts(self) -> (deltalake::DeltaTable, tokio::runtime::Runtime) {
        (self.table, self.runtime)
    }
}

pub fn normalize_path(path: &Path) -> Result<PathBuf, EngineError> {
    let raw = path.to_string_lossy();
    if raw.starts_with("file:") {
        let url = url::Url::parse(&raw).map_err(|error| {
            EngineError::invalid_input(format!("invalid Delta file URL: {error}"))
        })?;
        return url
            .to_file_path()
            .map_err(|()| EngineError::invalid_input("invalid Delta file URL path".into()));
    }
    Ok(path.to_path_buf())
}

pub fn path_to_table_uri(path: &Path) -> Result<String, EngineError> {
    let raw = path.to_string_lossy();
    if raw.starts_with("file://") {
        return Ok(raw.into_owned());
    }
    let absolute = if path.exists() {
        path.canonicalize().map_err(|error| {
            EngineError::invalid_input(format!("cannot canonicalize Delta path: {error}"))
        })?
    } else {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let name = path.file_name().unwrap_or(std::ffi::OsStr::new("."));
        if parent.exists() {
            parent
                .canonicalize()
                .map_err(|error| {
                    EngineError::invalid_input(format!(
                        "cannot canonicalize Delta path parent: {error}"
                    ))
                })?
                .join(name)
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        }
    };
    url::Url::from_file_path(&absolute)
        .map(|url| url.to_string())
        .map_err(|_| {
            EngineError::invalid_input(format!(
                "invalid local Delta path for file URL: {}",
                absolute.display()
            ))
        })
}

#[cfg(feature = "driver")]
pub fn load_snapshot(path: &Path, version: Option<i64>) -> Result<DeltaSnapshot, DeltaLoadError> {
    let path = normalize_path(path).map_err(DeltaLoadError::Failure)?;
    let table_uri = path_to_table_uri(&path).map_err(DeltaLoadError::Failure)?;
    load_snapshot_uri(&table_uri, version)
}

#[cfg(feature = "driver")]
pub fn load_snapshot_uri(
    table_uri: &str,
    version: Option<i64>,
) -> Result<DeltaSnapshot, DeltaLoadError> {
    let url = url::Url::parse(table_uri).map_err(|error| {
        DeltaLoadError::Failure(EngineError::invalid_input(format!(
            "invalid Delta table URI: {error}"
        )))
    })?;
    let version = version
        .map(|version| {
            u64::try_from(version).map_err(|_| {
                DeltaLoadError::Failure(EngineError::invalid_input(format!(
                    "invalid Delta version {version}"
                )))
            })
        })
        .transpose()?;
    let runtime = tokio::runtime::Runtime::new().map_err(|error| {
        DeltaLoadError::Failure(EngineError::internal(format!(
            "cannot start Delta runtime: {error}"
        )))
    })?;
    let table = runtime.block_on(async {
        let builder = deltalake::DeltaTableBuilder::from_url(url)?;
        if let Some(version) = version {
            builder.with_version(version).load().await
        } else {
            builder.load().await
        }
    });
    let table = match table {
        Ok(table) => table,
        Err(deltalake::DeltaTableError::NotATable(_)) => return Err(DeltaLoadError::NotATable),
        Err(error) => {
            return Err(DeltaLoadError::Failure(EngineError::internal(format!(
                "Delta snapshot load failed: {error}"
            ))));
        }
    };
    Ok(DeltaSnapshot {
        table,
        table_uri: table_uri.to_string(),
        runtime,
    })
}

/// Commit prepared Delta actions and return the exact version accepted by the log.
#[cfg(feature = "driver")]
pub async fn commit_actions(
    table: &mut deltalake::DeltaTable,
    actions: Vec<deltalake::kernel::Action>,
    mode: deltalake::protocol::SaveMode,
    partition_columns: &[String],
) -> Result<u64, deltalake::DeltaTableError> {
    use deltalake::kernel::transaction::CommitBuilder;
    use deltalake::protocol::DeltaOperation;

    let table_state = table.snapshot()?;
    let partition_columns = partition_columns.to_vec();
    let operation = DeltaOperation::Write {
        mode,
        partition_by: if partition_columns.is_empty() {
            None
        } else {
            Some(partition_columns)
        },
        predicate: None,
    };
    let finalized = CommitBuilder::default()
        .with_actions(actions)
        .build(Some(table_state), table.log_store().clone(), operation)
        .await?;
    let version = finalized.snapshot().version();
    table.state = Some(finalized.snapshot());
    Ok(version)
}

/// Create a Delta table from a converted schema and prepared file actions.
#[cfg(feature = "driver")]
pub async fn create_table(
    table_uri: impl Into<String>,
    columns: Vec<deltalake::kernel::StructField>,
    actions: Vec<deltalake::kernel::Action>,
) -> Result<deltalake::DeltaTable, deltalake::DeltaTableError> {
    deltalake::operations::create::CreateBuilder::new()
        .with_location(table_uri.into())
        .with_columns(columns)
        .with_actions(actions)
        .await
}
