//! DataFrame writer modes, options, catalog saves, and file output.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

fn next_external_write_id() -> String {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    format!(
        "local-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn external_write_failure(name: &str, error: robin_sparkless_core::EngineError) -> PolarsError {
    if error.category() == robin_sparkless_core::ErrorCategory::Cancelled {
        PolarsError::ComputeError(format!("cancelled: {error}").into())
    } else {
        let outcome = match error.retryability() {
            robin_sparkless_core::ErrorRetryability::Terminal => "terminal",
            robin_sparkless_core::ErrorRetryability::Retryable => "retryable",
            _ => "unspecified",
        };
        PolarsError::InvalidOperation(
            format!("EXTERNAL_WRITE_NOT_COMMITTED: {outcome}: provider failed to write '{name}': {error}").into(),
        )
    }
}

fn validate_external_write_outcome(
    name: &str,
    provider: &str,
    operation_id: &str,
    outcome: ExternalTableWriteOutcome,
    descriptor: &ExternalTableDescriptor,
) -> Result<(), PolarsError> {
    match outcome {
        ExternalTableWriteOutcome::Committed(result) => result.validate_against(descriptor).map_err(
            |error| {
                PolarsError::InvalidOperation(
                    format!(
                        "WRITE_OUTCOME_UNKNOWN: provider '{provider}' operation '{operation_id}' for '{name}' committed but returned an invalid receipt: {error}"
                    )
                    .into(),
                )
            },
        ),
        ExternalTableWriteOutcome::NotCommitted(error) => Err(external_write_failure(name, error)),
        ExternalTableWriteOutcome::OutcomeUnknown {
            provider,
            operation_id,
            message,
        } => Err(PolarsError::InvalidOperation(
            format!(
                "WRITE_OUTCOME_UNKNOWN: provider '{provider}' operation '{operation_id}' for '{name}': {message}"
            )
            .into(),
        )),
    }
}

/// Write mode: overwrite or append (PySpark DataFrameWriter.mode for path-based save).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Overwrite,
    Append,
}

/// Save mode for saveAsTable (PySpark default is ErrorIfExists).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SaveMode {
    /// Throw if table already exists (PySpark default).
    ErrorIfExists,
    /// Replace existing table.
    Overwrite,
    /// Append to existing table; create if not exists. Column names align.
    Append,
    /// No-op if table exists; create if not.
    Ignore,
}

/// Output format for generic write (PySpark DataFrameWriter.format).
#[derive(Clone, Copy)]
pub enum WriteFormat {
    Parquet,
    Csv,
    Json,
}

/// Align two DataFrames to a merged column order (existing first, then new columns not in existing).
/// Adds null columns for missing columns. Used for saveAsTable append with mergeSchema (issue #1109).
pub(crate) fn align_to_merged_schema_inline(
    existing: &PlDataFrame,
    new_df: &PlDataFrame,
) -> Result<(PlDataFrame, PlDataFrame), PolarsError> {
    use polars::prelude::*;
    let existing_names: Vec<String> = existing
        .get_column_names()
        .iter()
        .map(|s| s.as_str().to_string())
        .collect();
    let new_names: Vec<String> = new_df
        .get_column_names()
        .iter()
        .map(|s| s.as_str().to_string())
        .collect();
    let existing_set: HashSet<&str> = existing_names.iter().map(String::as_str).collect();
    let mut merged: Vec<String> = existing_names.clone();
    for n in &new_names {
        if !existing_set.contains(n.as_str()) {
            merged.push(n.clone());
        }
    }
    let n_existing = existing.height();
    let n_new = new_df.height();
    let schema_existing = existing.schema();
    let schema_new = new_df.schema();
    let name_into = |n: &String| n.as_str().into();
    let mut cols_existing: Vec<polars::prelude::Column> = Vec::with_capacity(merged.len());
    let mut cols_new: Vec<polars::prelude::Column> = Vec::with_capacity(merged.len());
    for name in &merged {
        if let Some(dtype) = schema_existing.get(name) {
            if let Some(idx) = existing.get_column_index(name) {
                cols_existing.push(existing.columns()[idx].clone());
            } else {
                cols_existing.push(Series::full_null(name_into(name), n_existing, dtype).into());
            }
        } else if let Some(dtype) = schema_new.get(name) {
            cols_existing.push(Series::full_null(name_into(name), n_existing, dtype).into());
        } else {
            cols_existing
                .push(Series::full_null(name_into(name), n_existing, &DataType::String).into());
        }
        if let Some(dtype) = schema_new.get(name) {
            if let Some(idx) = new_df.get_column_index(name) {
                cols_new.push(new_df.columns()[idx].clone());
            } else {
                cols_new.push(Series::full_null(name_into(name), n_new, dtype).into());
            }
        } else if let Some(dtype) = schema_existing.get(name) {
            cols_new.push(Series::full_null(name_into(name), n_new, dtype).into());
        } else {
            cols_new.push(Series::full_null(name_into(name), n_new, &DataType::String).into());
        }
    }
    let aligned_existing = PlDataFrame::new_infer_height(cols_existing)?;
    let aligned_new = PlDataFrame::new_infer_height(cols_new)?;
    Ok((aligned_existing, aligned_new))
}

/// Builder for writing DataFrame to path (PySpark DataFrameWriter).
pub struct DataFrameWriter<'a> {
    df: &'a DataFrame,
    mode: WriteMode,
    format: WriteFormat,
    format_was_set: bool,
    options: HashMap<String, String>,
    partition_by: Vec<String>,
}

impl<'a> DataFrameWriter<'a> {
    pub(super) fn new(df: &'a DataFrame) -> Self {
        Self {
            df,
            mode: WriteMode::Overwrite,
            format: WriteFormat::Parquet,
            format_was_set: false,
            options: HashMap::new(),
            partition_by: Vec::new(),
        }
    }

    pub fn mode(mut self, mode: WriteMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn format(mut self, format: WriteFormat) -> Self {
        self.format = format;
        self.format_was_set = true;
        self
    }

    /// Add a single option (PySpark: option(key, value)). Returns self for chaining.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Add multiple options (PySpark: options(**kwargs)). Returns self for chaining.
    pub fn options(mut self, opts: impl IntoIterator<Item = (String, String)>) -> Self {
        for (k, v) in opts {
            self.options.insert(k, v);
        }
        self
    }

    /// Partition output by the given columns (PySpark: partitionBy(cols)).
    pub fn partition_by(mut self, cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.partition_by = cols.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Save the DataFrame as a table (PySpark: saveAsTable). In-memory by default; when spark.sql.warehouse.dir is set, persists to disk for cross-session access.
    /// Writer options (e.g. mergeSchema) are applied when set via option()/options(). Issue #1109.
    pub fn save_as_table(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
    ) -> Result<(), PolarsError> {
        self.save_as_table_with_cancellation(session, name, mode, &CancellationToken::new())
    }

    /// Save a table while honoring cancellation before materialization and commit acceptance.
    pub fn save_as_table_with_cancellation(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        token: &CancellationToken,
    ) -> Result<(), PolarsError> {
        let opts: Vec<(String, String)> = self
            .options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let options = if opts.is_empty() {
            None
        } else {
            Some(opts.as_slice())
        };
        self.save_as_table_impl(session, name, mode, options, token)
    }

    /// Same as save_as_table but with options passed explicitly (e.g. from Python writer). Issue #1109.
    pub fn save_as_table_with_options(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        options: &[(String, String)],
    ) -> Result<(), PolarsError> {
        self.save_as_table_with_options_and_cancellation(
            session,
            name,
            mode,
            options,
            &CancellationToken::new(),
        )
    }

    /// Same as `save_as_table_with_options` with a caller-owned cancellation token.
    pub fn save_as_table_with_options_and_cancellation(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        options: &[(String, String)],
        token: &CancellationToken,
    ) -> Result<(), PolarsError> {
        self.save_as_table_impl(
            session,
            name,
            mode,
            if options.is_empty() {
                None
            } else {
                Some(options)
            },
            token,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_as_external_table_descriptor(
        &self,
        name: &str,
        mode: SaveMode,
        options: Option<&[(String, String)]>,
        catalog: &ExternalCatalog,
        ownership: &CatalogOwnership,
        descriptor: &ExternalTableDescriptor,
        logical_map_paths: &[String],
        operation_id: Option<&str>,
        token: &CancellationToken,
    ) -> Result<(), PolarsError> {
        token
            .check_cancelled()
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
        let external_mode = match mode {
            SaveMode::ErrorIfExists => {
                return Err(PolarsError::InvalidOperation(
                    format!("Table or view '{name}' already exists. SaveMode is ErrorIfExists.")
                        .into(),
                ));
            }
            SaveMode::Ignore => return Ok(()),
            SaveMode::Append => ExternalTableSaveMode::Append,
            SaveMode::Overwrite => ExternalTableSaveMode::Overwrite,
        };
        fn contains_map(dtype: &crate::schema::DataType) -> bool {
            use crate::schema::DataType;
            match dtype {
                DataType::Map(_, _) | DataType::MapWithNullability { .. } => true,
                DataType::Array(element)
                | DataType::ArrayWithNullability {
                    element_type: element,
                    ..
                } => contains_map(element),
                DataType::Struct(fields) => {
                    fields.iter().any(|field| contains_map(&field.data_type))
                }
                DataType::LogicalStruct(fields) => {
                    fields.iter().any(|field| contains_map(&field.data_type))
                }
                _ => false,
            }
        }
        if !logical_map_paths.is_empty()
            || descriptor
                .schema
                .logical_fields()
                .iter()
                .any(|field| contains_map(&field.data_type))
        {
            return Err(PolarsError::InvalidOperation(
                "external catalog Arrow IPC writes do not yet support logical Map columns".into(),
            ));
        }

        let table_format = options
            .unwrap_or_default()
            .iter()
            .rev()
            .find(|(key, _)| key.eq_ignore_ascii_case("sparkless.writer.format"))
            .map(|(_, value)| value.clone())
            .or_else(|| {
                self.format_was_set.then(|| match self.format {
                    WriteFormat::Parquet => "parquet".to_string(),
                    WriteFormat::Csv => "csv".to_string(),
                    WriteFormat::Json => "json".to_string(),
                })
            });
        let writer_options = options
            .unwrap_or_default()
            .iter()
            .filter(|(key, _)| !key.to_ascii_lowercase().starts_with("sparkless."))
            .cloned()
            .collect();
        let partition_by = self
            .partition_by
            .iter()
            .map(|column| self.df.resolve_column_name(column))
            .collect::<Result<Vec<_>, _>>()?;

        if !catalog.has_batch_writer_for(ownership) {
            return Err(PolarsError::InvalidOperation(
                "external namespace has no bounded batch writer capability".into(),
            ));
        }
        let max_rows = external_arrow_batch_limit(
            options,
            "sparkless.writer.maxRowsPerBatch",
            DEFAULT_EXTERNAL_ARROW_BATCH_ROWS,
        )?;
        let max_bytes = external_arrow_batch_limit(
            options,
            "sparkless.writer.maxBytesPerBatch",
            DEFAULT_EXTERNAL_ARROW_BATCH_BYTES,
        )?;
        let batches = self
            .df
            .to_arrow_ipc_batches_with_cancellation(max_rows, max_bytes, token)
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
        let request = ExternalTableWriteBatchRequest {
            operation_id: operation_id
                .map(str::to_owned)
                .unwrap_or_else(next_external_write_id),
            identifier: name.to_string(),
            table: descriptor.clone(),
            batches,
            mode: external_mode,
            format: table_format,
            options: writer_options,
            partition_by,
        };
        token
            .check_cancelled()
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
        let write_operation_id = request.operation_id.clone();
        let outcome = catalog
            .write_table_batches(ownership, request, token)
            .map_err(|error| external_write_failure(name, error))?;
        validate_external_write_outcome(
            name,
            &descriptor.provider,
            &write_operation_id,
            outcome,
            descriptor,
        )
    }

    /// JDBC write (PySpark-style). Requires the `jdbc` or `sqlite` feature.
    #[cfg(any(
        feature = "jdbc",
        feature = "jdbc_mysql",
        feature = "jdbc_mariadb",
        feature = "jdbc_mssql",
        feature = "jdbc_oracle",
        feature = "jdbc_db2",
        feature = "sqlite"
    ))]
    pub fn jdbc(
        &self,
        url: &str,
        table: &str,
        properties: &[(String, String)],
        mode: SaveMode,
    ) -> Result<(), crate::error::EngineError> {
        use crate::jdbc::{JdbcOptions, write_jdbc_from_polars};
        use std::collections::HashMap;

        let mut props_map = HashMap::new();
        for (k, v) in properties {
            props_map.insert(k.clone(), v.clone());
        }
        let opts = JdbcOptions::from_url_dbtable_and_properties(
            url.to_string(),
            table.to_string(),
            &props_map,
        )?;
        let pl_df = self
            .df
            .collect_inner()
            .map_err(crate::polars_to_core_error)?;
        write_jdbc_from_polars(pl_df.as_ref(), &opts, mode)
    }

    fn save_as_table_impl(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
        options: Option<&[(String, String)]>,
        token: &CancellationToken,
    ) -> Result<(), PolarsError> {
        use polars::prelude::*;
        use std::fs;
        use std::path::Path;

        token
            .check_cancelled()
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;

        if SparkSession::is_global_temp_name(name) && session.local_table_exists(name) {
            return Err(PolarsError::InvalidOperation(
                format!("cannot save a table over global temporary view '{name}'").into(),
            ));
        }

        let (merge_schema, overwrite_schema, is_delta_format, logical_map_paths) =
            options.map_or((false, false, false, Vec::new()), |opts| {
                let mut merge_schema = false;
                let mut overwrite_schema = false;
                let mut is_delta = false;
                let mut logical_map_paths: Vec<String> = Vec::new();
                for (k, v) in opts {
                    if k.eq_ignore_ascii_case("mergeSchema") && v.eq_ignore_ascii_case("true") {
                        merge_schema = true;
                    } else if k.eq_ignore_ascii_case("overwriteSchema")
                        && v.eq_ignore_ascii_case("true")
                    {
                        overwrite_schema = true;
                    } else if k.eq_ignore_ascii_case("sparkless.writer.format")
                        && v.eq_ignore_ascii_case("delta")
                    {
                        is_delta = true;
                    } else if k.eq_ignore_ascii_case("sparkless.delta.logicalMapPaths") {
                        logical_map_paths = serde_json::from_str(v).unwrap_or_default();
                    }
                }
                (merge_schema, overwrite_schema, is_delta, logical_map_paths)
            });

        // A session-local table or view shadows the external catalog. Otherwise, an exact
        // provider hit owns this write; provider errors or unsupported writes must never fall
        // through into Sparkless' in-memory or warehouse writer.
        if !session.saved_table_exists(name)
            && !session.has_exact_temp_view(name)
            && !SparkSession::is_global_temp_name(name)
            && let Some(catalog) = session.external_catalog()?
        {
            let ownership = catalog.resolve_ownership(name).map_err(|error| {
                PolarsError::InvalidOperation(
                    format!("external catalog failed to resolve write owner for '{name}': {error}")
                        .into(),
                )
            })?;
            if matches!(&ownership, CatalogOwnership::External { .. }) {
                session.reject_read_only_external_owner(&ownership)?;
                let descriptor = catalog.resolve_table(&ownership, name).map_err(|error| {
                    PolarsError::InvalidOperation(
                        format!("external catalog failed to resolve '{name}': {error}").into(),
                    )
                })?;
                token
                    .check_cancelled()
                    .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
                let descriptor = descriptor.ok_or_else(|| {
                    PolarsError::InvalidOperation(
                        format!(
                            "external namespace owns '{name}', but the table does not exist and no create-table contract is available"
                        )
                        .into(),
                    )
                })?;
                descriptor.validate().map_err(|error| {
                    PolarsError::InvalidOperation(
                        format!(
                            "external catalog returned an invalid descriptor for '{name}': {error}"
                        )
                        .into(),
                    )
                })?;
                return self.save_as_external_table_descriptor(
                    name,
                    mode,
                    options,
                    catalog.as_ref(),
                    &ownership,
                    &descriptor,
                    &logical_map_paths,
                    None,
                    token,
                );
            }
        }

        // Warehouse paths (disk-backed saveAsTable). For schema-qualified names, allow both:
        // - "{warehouse}/{schema.table}" (single dir)
        // - "{warehouse}/{schema}/{table}" (schema as subdir)
        let warehouse_paths: Vec<std::path::PathBuf> = session
            .warehouse_dir()
            .map(|w| {
                let mut out = Vec::with_capacity(2);
                if let Ok(p) = robin_sparkless_core::resolve_path_under_base(w, name) {
                    out.push(p);
                }
                if name.contains('.') {
                    let alt = name.replace('.', std::path::MAIN_SEPARATOR_STR);
                    if let Ok(p) = robin_sparkless_core::resolve_path_under_base(w, &alt) {
                        out.push(p);
                    }
                }
                out
            })
            .unwrap_or_default();
        let warehouse_exists = warehouse_paths.iter().any(|p| p.is_dir());

        // Delta saveAsTable is warehouse-backed. Let the transaction snapshot determine whether
        // the target exists as a Delta table; an empty warehouse directory is only a location.
        if is_delta_format {
            #[cfg(feature = "delta")]
            {
                let target = warehouse_paths
                    .iter()
                    .find(|path| path.join("_delta_log").is_dir())
                    .or_else(|| warehouse_paths.first())
                    .ok_or_else(|| {
                        PolarsError::InvalidOperation(
                            "saveAsTable(format=delta) requires spark.sql.warehouse.dir to be set."
                                .into(),
                        )
                    })?;

                match mode {
                    SaveMode::Ignore if session.saved_table_exists(name) => return Ok(()),
                    SaveMode::ErrorIfExists if session.saved_table_exists(name) => {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "Table or view '{name}' already exists. SaveMode is ErrorIfExists."
                            )
                            .into(),
                        ));
                    }
                    SaveMode::Append
                    | SaveMode::Overwrite
                    | SaveMode::Ignore
                    | SaveMode::ErrorIfExists => {}
                }
                let pl_df = self
                    .df
                    .collect_with_cancellation(token)
                    .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?
                    .as_ref()
                    .clone();
                token
                    .check_cancelled()
                    .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
                let resolved_partition_by = self
                    .partition_by
                    .iter()
                    .map(|name| self.df.resolve_column_name(name))
                    .collect::<Result<Vec<_>, _>>()?;
                crate::delta::write_delta_with_save_options_and_logical_map_paths_for_catalog(
                    &pl_df,
                    target,
                    mode,
                    merge_schema,
                    overwrite_schema,
                    &resolved_partition_by,
                    &logical_map_paths,
                )?;
                let loaded = crate::delta::read_delta(target, session.is_case_sensitive())?;
                session.register_table(name, loaded);
                return Ok(());
            }
            #[cfg(not(feature = "delta"))]
            {
                let _ = (
                    warehouse_paths,
                    warehouse_exists,
                    merge_schema,
                    overwrite_schema,
                    logical_map_paths,
                );
                return Err(PolarsError::InvalidOperation(
                    "saveAsTable(format=delta) requires the delta feature to be enabled.".into(),
                ));
            }
        }

        let warehouse_path = warehouse_paths.first().cloned();

        fn persist_to_warehouse(
            df: &crate::dataframe::DataFrame,
            dir: &Path,
        ) -> Result<(), PolarsError> {
            use std::fs;
            fs::create_dir_all(dir).map_err(|e| {
                PolarsError::ComputeError(format!("saveAsTable: create dir: {e}").into())
            })?;
            let file_path = dir.join("data.parquet");
            df.write()
                .mode(crate::dataframe::WriteMode::Overwrite)
                .format(crate::dataframe::WriteFormat::Parquet)
                .save(&file_path)
        }

        fn check_cancelled(token: &CancellationToken) -> Result<(), PolarsError> {
            token
                .check_cancelled()
                .map_err(|error| PolarsError::ComputeError(error.to_string().into()))
        }

        fn staging_path(target: &Path) -> std::path::PathBuf {
            static NEXT_STAGE_ID: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let id = NEXT_STAGE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let parent = target.parent().unwrap_or_else(|| Path::new("."));
            let name = target
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("table");
            parent.join(format!(
                ".{name}.sparkless-stage-{}-{id}",
                std::process::id()
            ))
        }

        fn stage_warehouse_write(
            df: &crate::dataframe::DataFrame,
            target: &Path,
            token: &CancellationToken,
        ) -> Result<std::path::PathBuf, PolarsError> {
            let materialized = df
                .collect_with_cancellation(token)
                .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
            check_cancelled(token)?;
            let eager = crate::dataframe::DataFrame::from_polars_with_options(
                materialized.as_ref().clone(),
                df.case_sensitive,
            );
            let stage = staging_path(target);
            if let Err(error) = persist_to_warehouse(&eager, &stage) {
                let _ = fs::remove_dir_all(&stage);
                return Err(error);
            }
            if let Err(error) = check_cancelled(token) {
                let _ = fs::remove_dir_all(&stage);
                return Err(error);
            }
            Ok(stage)
        }

        fn commit_warehouse_stage(
            stage: &Path,
            target: &Path,
            overwrite: bool,
        ) -> Result<(), PolarsError> {
            let backup = staging_path(target);
            let had_target = target.exists();
            if had_target && !overwrite {
                let _ = fs::remove_dir_all(stage);
                return Err(PolarsError::InvalidOperation(
                    format!("saveAsTable target already exists: {}", target.display()).into(),
                ));
            }
            if had_target {
                if let Err(error) = fs::rename(target, &backup) {
                    let _ = fs::remove_dir_all(stage);
                    return Err(PolarsError::ComputeError(
                        format!("saveAsTable: move existing target aside: {error}").into(),
                    ));
                }
            }
            if let Err(error) = fs::rename(stage, target) {
                if had_target {
                    let _ = fs::rename(&backup, target);
                }
                let _ = fs::remove_dir_all(stage);
                return Err(PolarsError::ComputeError(
                    format!("saveAsTable: commit staged directory: {error}").into(),
                ));
            }
            if had_target {
                let _ = fs::remove_dir_all(backup);
            }
            Ok(())
        }

        let final_df = match mode {
            SaveMode::ErrorIfExists => {
                if session.saved_table_exists(name) || warehouse_exists {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "Table or view '{name}' already exists. SaveMode is ErrorIfExists."
                        )
                        .into(),
                    ));
                }
                if let Some(ref p) = warehouse_path {
                    let stage = stage_warehouse_write(self.df, p, token)?;
                    check_cancelled(token)?;
                    commit_warehouse_stage(&stage, p, false)?;
                }
                self.df.clone()
            }
            SaveMode::Overwrite => {
                if let Some(ref p) = warehouse_path {
                    let stage = stage_warehouse_write(self.df, p, token)?;
                    check_cancelled(token)?;
                    commit_warehouse_stage(&stage, p, true)?;
                }
                self.df.clone()
            }
            SaveMode::Append => {
                let saved_existing = session.get_saved_table(name);
                if let (Some(existing), Some(incoming)) = (
                    saved_existing
                        .as_ref()
                        .and_then(|frame| frame.logical_schema.as_ref()),
                    self.df.logical_schema.as_ref(),
                ) {
                    let same_name = |left: &str, right: &str| {
                        if self.df.case_sensitive {
                            left == right
                        } else {
                            left.eq_ignore_ascii_case(right)
                        }
                    };
                    for existing_field in existing.logical_fields() {
                        let incoming_field = incoming
                            .logical_fields()
                            .iter()
                            .find(|field| same_name(&field.name, &existing_field.name));
                        if incoming_field.is_none() && !merge_schema {
                            return Err(PolarsError::SchemaMismatch(
                                format!(
                                    "saveAsTable append: logical field '{}' is missing from incoming schema for table '{name}'",
                                    existing_field.name
                                )
                                .into(),
                            ));
                        }
                        if let Some(incoming_field) = incoming_field {
                            if serde_json::to_value(&existing_field.data_type).ok()
                                != serde_json::to_value(&incoming_field.data_type).ok()
                                || existing_field.metadata != incoming_field.metadata
                            {
                                return Err(PolarsError::SchemaMismatch(
                                    format!(
                                        "saveAsTable append: logical field '{}' does not match existing table '{name}'",
                                        existing_field.name
                                    )
                                    .into(),
                                ));
                            }
                        }
                    }
                    if !merge_schema
                        && incoming.logical_fields().iter().any(|incoming_field| {
                            !existing
                                .logical_fields()
                                .iter()
                                .any(|field| same_name(&field.name, &incoming_field.name))
                        })
                    {
                        return Err(PolarsError::SchemaMismatch(
                            format!(
                                "saveAsTable append: incoming logical schema has extra fields for table '{name}'"
                            )
                            .into(),
                        ));
                    }
                }
                let existing_pl = if let Some(existing) = saved_existing.as_ref() {
                    existing.collect_inner()?.as_ref().clone()
                } else if warehouse_exists {
                    // Read from warehouse (data.parquet convention). For schema-qualified names,
                    // look in both "{warehouse}/{schema.table}" and "{warehouse}/{schema}/{table}".
                    let mut loaded: Option<polars::prelude::DataFrame> = None;
                    for p in &warehouse_paths {
                        if !p.is_dir() {
                            continue;
                        }
                        let data_file = p.join("data.parquet");
                        let read_path = if data_file.is_file() {
                            data_file.as_path()
                        } else {
                            p.as_path()
                        };
                        let pl_path = polars::prelude::PlRefPath::try_from_path(read_path)
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("saveAsTable append: path: {e}").into(),
                                )
                            })?;
                        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("saveAsTable append: read warehouse: {e}").into(),
                                )
                            })?;
                        loaded = Some(lf.collect().map_err(|e| {
                            PolarsError::ComputeError(
                                format!("saveAsTable append: collect: {e}").into(),
                            )
                        })?);
                        break;
                    }
                    loaded.ok_or_else(|| {
                        PolarsError::ComputeError(
                            format!("saveAsTable append: warehouse table '{name}' not found")
                                .into(),
                        )
                    })?
                } else {
                    // New table
                    if let Some(ref p) = warehouse_path {
                        let stage = stage_warehouse_write(self.df, p, token)?;
                        check_cancelled(token)?;
                        commit_warehouse_stage(&stage, p, false)?;
                    } else {
                        check_cancelled(token)?;
                    }
                    session.register_table(name, self.df.clone());
                    return Ok(());
                };
                let new_pl = self
                    .df
                    .collect_with_cancellation(token)
                    .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?
                    .as_ref()
                    .clone();
                if let Some(schema) = saved_existing
                    .as_ref()
                    .and_then(|existing| existing.logical_schema.as_ref())
                {
                    for field in schema.logical_fields() {
                        let column = match new_pl.column(&field.name) {
                            Ok(column) => column,
                            Err(_) if merge_schema => {
                                if !field.nullable {
                                    return Err(PolarsError::InvalidOperation(
                                        format!("saveAsTable append: non-nullable field '{}' contains null values", field.name).into(),
                                    ));
                                }
                                continue;
                            }
                            Err(_) => continue,
                        };
                        for row in 0..new_pl.height() {
                            let value = column.get(row)?;
                            validate_logical_nullability(
                                &value,
                                &field.data_type,
                                field.nullable,
                                &field.name,
                            )?;
                        }
                    }
                }
                check_cancelled(token)?;
                let existing_for_schema = saved_existing.clone().unwrap_or_else(|| {
                    crate::dataframe::DataFrame::from_polars_with_options(
                        existing_pl.clone(),
                        self.df.case_sensitive,
                    )
                });
                let merged = if merge_schema {
                    let (aligned_existing, aligned_new) =
                        align_to_merged_schema_inline(&existing_pl, &new_pl)?;
                    let mut out = aligned_existing;
                    out.vstack_mut(&aligned_new)?;
                    crate::dataframe::DataFrame::from_polars_with_options(
                        out,
                        self.df.case_sensitive,
                    )
                } else {
                    let existing_cols: Vec<&str> = existing_pl
                        .get_column_names()
                        .iter()
                        .map(|s| s.as_str())
                        .collect();
                    let new_cols = new_pl.get_column_names();
                    let missing: Vec<_> = existing_cols
                        .iter()
                        .filter(|c| !new_cols.iter().any(|n| n.as_str() == **c))
                        .collect();
                    if !missing.is_empty() {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "saveAsTable append: new DataFrame missing columns: {:?}",
                                missing
                            )
                            .into(),
                        ));
                    }
                    let new_ordered = new_pl.select(existing_cols.iter().copied())?;
                    let mut combined = existing_pl;
                    combined.vstack_mut(&new_ordered)?;
                    crate::dataframe::DataFrame::from_polars_with_options(
                        combined,
                        self.df.case_sensitive,
                    )
                };
                let merged = if let Some(logical_schema) =
                    existing_for_schema.merged_logical_schema(self.df, &merged, merge_schema)
                {
                    merged.with_logical_schema(logical_schema)?
                } else {
                    merged
                };
                if let Some(ref p) = warehouse_path {
                    let stage = stage_warehouse_write(&merged, p, token)?;
                    check_cancelled(token)?;
                    commit_warehouse_stage(&stage, p, true)?;
                }
                merged
            }
            SaveMode::Ignore => {
                if session.saved_table_exists(name) || warehouse_exists {
                    return Ok(());
                }
                if let Some(ref p) = warehouse_path {
                    let stage = stage_warehouse_write(self.df, p, token)?;
                    check_cancelled(token)?;
                    commit_warehouse_stage(&stage, p, false)?;
                }
                self.df.clone()
            }
        };
        if warehouse_path.is_none() {
            check_cancelled(token)?;
        }
        session.register_table(name, final_df);
        Ok(())
    }

    /// Write as Parquet (PySpark: parquet(path)). Equivalent to format(Parquet).save(path).
    pub fn parquet(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Parquet,
            format_was_set: true,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write as CSV (PySpark: csv(path)). Equivalent to format(Csv).save(path).
    pub fn csv(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Csv,
            format_was_set: true,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write as JSON lines (PySpark: json(path)). Equivalent to format(Json).save(path).
    pub fn json(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Json,
            format_was_set: true,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write to path. Overwrite replaces; append reads existing (if any) and concatenates then writes.
    /// With partition_by, path is a directory; each partition is written as path/col1=val1/col2=val2/... with partition columns omitted from the file (Spark/Hive style).
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        use polars::prelude::*;
        let path = robin_sparkless_core::maybe_confine_files_path(path.as_ref()).map_err(|e| {
            PolarsError::InvalidOperation(format!("write path confinement: {e}").into())
        })?;
        let path = path.as_path();
        let to_write: PlDataFrame = match self.mode {
            WriteMode::Overwrite => self.df.collect_inner()?.as_ref().clone(),
            WriteMode::Append => {
                if self.partition_by.is_empty() {
                    let existing: Option<PlDataFrame> = if path.exists() && path.is_file() {
                        match self.format {
                            WriteFormat::Parquet => polars::prelude::PlRefPath::try_from_path(path)
                                .ok()
                                .and_then(|pl_path| {
                                    LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
                                        .and_then(|lf| lf.collect())
                                        .ok()
                                }),
                            WriteFormat::Csv => polars::prelude::PlRefPath::try_from_path(path)
                                .ok()
                                .and_then(|pl_path| {
                                    LazyCsvReader::new(pl_path)
                                        .with_has_header(true)
                                        .finish()
                                        .and_then(|lf| lf.collect())
                                        .ok()
                                }),
                            WriteFormat::Json => polars::prelude::PlRefPath::try_from_path(path)
                                .ok()
                                .and_then(|pl_path| {
                                    LazyJsonLineReader::new(pl_path)
                                        .finish()
                                        .and_then(|lf| lf.collect())
                                        .ok()
                                }),
                        }
                    } else {
                        None
                    };
                    match existing {
                        Some(existing) => {
                            let lfs: [LazyFrame; 2] = [
                                existing.clone().lazy(),
                                self.df.collect_inner()?.as_ref().clone().lazy(),
                            ];
                            concat(lfs, UnionArgs::default())?.collect()?
                        }
                        None => self.df.collect_inner()?.as_ref().clone(),
                    }
                } else {
                    self.df.collect_inner()?.as_ref().clone()
                }
            }
        };

        if !self.partition_by.is_empty() {
            return self.save_partitioned(path, &to_write);
        }

        // When path is an existing directory (e.g. Delta table root from mkdtemp), write part file inside it.
        let ext = match self.format {
            WriteFormat::Parquet => "parquet",
            WriteFormat::Csv => "csv",
            WriteFormat::Json => "json",
        };
        let out_path = if path.exists() && path.is_dir() {
            let part_idx = if self.mode == WriteMode::Append {
                let suffix = format!(".{ext}");
                let max_n = std::fs::read_dir(path)
                    .map(|rd| {
                        rd.filter_map(Result::ok)
                            .filter_map(|e| {
                                e.file_name().to_str().and_then(|s| {
                                    s.strip_prefix("part-")
                                        .and_then(|t| t.strip_suffix(&suffix))
                                        .and_then(|t| t.parse::<u32>().ok())
                                })
                            })
                            .max()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0);
                max_n + 1
            } else {
                0
            };
            path.join(format!("part-{part_idx:05}.{ext}"))
        } else {
            path.to_path_buf()
        };

        match self.format {
            WriteFormat::Parquet => {
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        PolarsError::ComputeError(format!("write parquet create dir: {e}").into())
                    })?;
                }
                let mut file = std::fs::File::create(&out_path).map_err(|e| {
                    PolarsError::ComputeError(format!("write parquet create: {e}").into())
                })?;
                let mut df_mut = to_write;
                ParquetWriter::new(&mut file)
                    .finish(&mut df_mut)
                    .map_err(|e| PolarsError::ComputeError(format!("write parquet: {e}").into()))?;
            }
            WriteFormat::Csv => {
                let has_header = self
                    .options
                    .get("header")
                    .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                    .unwrap_or(true);
                let delimiter = self
                    .options
                    .get("sep")
                    .and_then(|s| s.bytes().next())
                    .unwrap_or(b',');
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        PolarsError::ComputeError(format!("write csv create dir: {e}").into())
                    })?;
                }
                let mut file = std::fs::File::create(&out_path).map_err(|e| {
                    PolarsError::ComputeError(format!("write csv create: {e}").into())
                })?;
                CsvWriter::new(&mut file)
                    .include_header(has_header)
                    .with_separator(delimiter)
                    .finish(&mut to_write.clone())
                    .map_err(|e| PolarsError::ComputeError(format!("write csv: {e}").into()))?;
            }
            WriteFormat::Json => {
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        PolarsError::ComputeError(format!("write json create dir: {e}").into())
                    })?;
                }
                let mut file = std::fs::File::create(&out_path).map_err(|e| {
                    PolarsError::ComputeError(format!("write json create: {e}").into())
                })?;
                JsonWriter::new(&mut file)
                    .finish(&mut to_write.clone())
                    .map_err(|e| PolarsError::ComputeError(format!("write json: {e}").into()))?;
            }
        }
        Ok(())
    }

    /// Write partitioned by columns: path/col1=val1/col2=val2/part-00000.{ext}. Partition columns are not written into the file (Spark/Hive style).
    fn save_partitioned(&self, path: &Path, to_write: &PlDataFrame) -> Result<(), PolarsError> {
        use polars::prelude::*;
        let resolved: Vec<String> = self
            .partition_by
            .iter()
            .map(|c| self.df.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let all_names = to_write.get_column_names();
        let data_cols: Vec<&str> = all_names
            .iter()
            .filter(|n| !resolved.iter().any(|r| r == n.as_str()))
            .map(|n| n.as_str())
            .collect();

        let unique_keys = to_write
            .select(resolved.iter().map(|s| s.as_str()).collect::<Vec<_>>())?
            .unique::<Option<&[String]>, String>(
                None,
                polars::prelude::UniqueKeepStrategy::First,
                None,
            )?;

        if self.mode == WriteMode::Overwrite && path.exists() {
            if path.is_dir() {
                std::fs::remove_dir_all(path).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write partitioned: remove_dir_all: {e}").into(),
                    )
                })?;
            } else {
                std::fs::remove_file(path).map_err(|e| {
                    PolarsError::ComputeError(format!("write partitioned: remove_file: {e}").into())
                })?;
            }
        }
        std::fs::create_dir_all(path).map_err(|e| {
            PolarsError::ComputeError(format!("write partitioned: create_dir_all: {e}").into())
        })?;

        let ext = match self.format {
            WriteFormat::Parquet => "parquet",
            WriteFormat::Csv => "csv",
            WriteFormat::Json => "json",
        };

        for row_idx in 0..unique_keys.height() {
            let row = unique_keys
                .get(row_idx)
                .ok_or_else(|| PolarsError::ComputeError("partition_row: get row".into()))?;
            let filter_expr = partition_row_to_filter_expr(&resolved, &row)?;
            let subset = to_write.clone().lazy().filter(filter_expr).collect()?;
            let subset = subset.select(data_cols.iter().copied())?;
            if subset.height() == 0 {
                continue;
            }

            let partition_segments = resolved
                .iter()
                .zip(row.iter())
                .map(|(name, av)| format_partition_value(av).map(|value| format!("{name}={value}")))
                .collect::<Result<Vec<_>, _>>()?;
            let part_path = partition_segments
                .into_iter()
                .fold(path.to_path_buf(), |p, seg| p.join(seg));
            std::fs::create_dir_all(&part_path).map_err(|e| {
                PolarsError::ComputeError(
                    format!("write partitioned: create_dir_all partition: {e}").into(),
                )
            })?;

            let file_idx = if self.mode == WriteMode::Append {
                let suffix = format!(".{ext}");
                let max_n = std::fs::read_dir(&part_path)
                    .map(|rd| {
                        rd.filter_map(Result::ok)
                            .filter_map(|e| {
                                e.file_name().to_str().and_then(|s| {
                                    s.strip_prefix("part-")
                                        .and_then(|t| t.strip_suffix(&suffix))
                                        .and_then(|t| t.parse::<u32>().ok())
                                })
                            })
                            .max()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0);
                max_n + 1
            } else {
                0
            };
            let filename = format!("part-{file_idx:05}.{ext}");
            let file_path = part_path.join(&filename);

            match self.format {
                WriteFormat::Parquet => {
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned parquet create: {e}").into(),
                        )
                    })?;
                    let mut df_mut = subset;
                    ParquetWriter::new(&mut file)
                        .finish(&mut df_mut)
                        .map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write partitioned parquet: {e}").into(),
                            )
                        })?;
                }
                WriteFormat::Csv => {
                    let has_header = self
                        .options
                        .get("header")
                        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                        .unwrap_or(true);
                    let delimiter = self
                        .options
                        .get("sep")
                        .and_then(|s| s.bytes().next())
                        .unwrap_or(b',');
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned csv create: {e}").into(),
                        )
                    })?;
                    CsvWriter::new(&mut file)
                        .include_header(has_header)
                        .with_separator(delimiter)
                        .finish(&mut subset.clone())
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("write partitioned csv: {e}").into())
                        })?;
                }
                WriteFormat::Json => {
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned json create: {e}").into(),
                        )
                    })?;
                    JsonWriter::new(&mut file)
                        .finish(&mut subset.clone())
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("write partitioned json: {e}").into())
                        })?;
                }
            }
        }
        Ok(())
    }
}
