//! Delta Lake read/write support (optional `delta` feature).
//! Uses deltalake (delta-rs) with Polars for execution.

use crate::dataframe::DataFrame;
use polars::prelude::{IntoLazy, LazyFrame, PlRefPath, PolarsError, ScanArgsParquet, UnionArgs};
use std::path::Path;
use url::Url;

/// Concatenate LazyFrames with diagonal strategy so differing schemas align (missing columns as null).
#[cfg(feature = "delta")]
fn concat_lazy_frames_diagonal(lfs: Vec<LazyFrame>) -> Result<LazyFrame, PolarsError> {
    if lfs.is_empty() {
        return Err(PolarsError::ComputeError("read_delta: no files".into()));
    }
    let args = UnionArgs {
        diagonal: true,
        ..UnionArgs::default()
    };
    polars::prelude::concat(lfs, args)
}

/// Read a Delta table at the given path (latest version).
/// Path can be a local path (e.g. `/tmp/table`) or a file URL (`file:///tmp/table`).
#[cfg(feature = "delta")]
pub fn read_delta(path: impl AsRef<Path>, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    read_delta_with_version(path, None, case_sensitive)
}

#[cfg(feature = "delta")]
pub fn logical_map_paths(path: impl AsRef<Path>) -> Result<Vec<String>, PolarsError> {
    use deltalake::DeltaTableBuilder;
    use tokio::runtime::Runtime;

    let normalized = normalize_delta_path(path.as_ref())?;
    let path = robin_sparkless_core::maybe_confine_files_path(&normalized).map_err(|e| {
        PolarsError::InvalidOperation(format!("read_delta path confinement: {e}").into())
    })?;
    // Delta readers also accept plain Parquet directories as a compatibility fallback. There is
    // no logical Delta schema to inspect when the log directory is absent (or is a file).
    if !path.join("_delta_log").is_dir() {
        return Ok(Vec::new());
    }
    let table_uri = path_to_table_uri(path.as_path())?;
    let url = Url::parse(table_uri.as_str()).map_err(|e| {
        PolarsError::ComputeError(format!("read_delta: invalid table URI: {e}").into())
    })?;
    let runtime = Runtime::new().map_err(|e| {
        PolarsError::ComputeError(format!("read_delta: failed to create runtime: {e}").into())
    })?;
    let table = match runtime.block_on(async { DeltaTableBuilder::from_url(url)?.load().await }) {
        Ok(table) => table,
        Err(error) if is_not_a_table(&error) => return Ok(Vec::new()),
        Err(error) => {
            return Err(PolarsError::ComputeError(
                format!("read_delta: {error}").into(),
            ));
        }
    };
    let schema = table
        .snapshot()
        .map_err(|e| PolarsError::ComputeError(format!("read_delta: snapshot: {e}").into()))?
        .metadata()
        .parse_schema()
        .map_err(|e| PolarsError::ComputeError(format!("read_delta: parse schema: {e}").into()))?;
    Ok(logical_map_paths_from_schema(&schema))
}

#[cfg(feature = "delta")]
pub fn logical_map_paths_from_schema(schema: &deltalake::kernel::StructType) -> Vec<String> {
    use deltalake::kernel::DataType;

    fn field_path(parent: &str, name: &str) -> String {
        let encoded = serde_json::to_string(name).expect("field names serialize as JSON strings");
        if parent.is_empty() {
            encoded
        } else {
            format!("{parent}.{encoded}")
        }
    }
    fn visit_type(data_type: &DataType, path: &str, paths: &mut Vec<String>) {
        match data_type {
            DataType::Map(map) => {
                paths.push(path.to_string());
                visit_type(&map.key_type, &format!("{path}{{key}}"), paths);
                visit_type(&map.value_type, &format!("{path}{{value}}"), paths);
            }
            DataType::Array(array) => visit_type(&array.element_type, &format!("{path}[]"), paths),
            DataType::Struct(fields) | DataType::Variant(fields) => {
                for field in fields.fields() {
                    visit_type(&field.data_type, &field_path(path, &field.name), paths);
                }
            }
            DataType::Primitive(_) => {}
        }
    }
    let mut paths = Vec::new();
    for field in schema.fields() {
        visit_type(&field.data_type, &field_path("", &field.name), &mut paths);
    }
    paths
}

const PARQUET_WALK_MAX_DEPTH: u32 = 64;

/// Recursively collect paths of all .parquet files under `dir` (depth-limited).
#[cfg(feature = "delta")]
fn collect_parquet_paths_under(dir: &Path) -> Result<Vec<std::path::PathBuf>, PolarsError> {
    collect_parquet_paths_under_depth(dir, 0)
}

#[cfg(feature = "delta")]
fn collect_parquet_paths_under_depth(
    dir: &Path,
    depth: u32,
) -> Result<Vec<std::path::PathBuf>, PolarsError> {
    if depth > PARQUET_WALK_MAX_DEPTH {
        return Err(PolarsError::ComputeError(
            format!(
                "read_delta fallback: directory depth exceeds {}",
                PARQUET_WALK_MAX_DEPTH
            )
            .into(),
        ));
    }
    let mut out = Vec::new();
    let entries = std::fs::read_dir(dir).map_err(|e| {
        PolarsError::ComputeError(format!("read_delta fallback read_dir: {}", e).into())
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            PolarsError::ComputeError(format!("read_delta fallback read_dir entry: {}", e).into())
        })?;
        let p = entry.path();
        let file_type = entry.file_type().map_err(|e| {
            PolarsError::ComputeError(format!("read_delta fallback file type: {e}").into())
        })?;
        if file_type.is_symlink() {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "Delta Parquet path contains a symbolic link: {}",
                    p.display()
                )
                .into(),
            ));
        }
        if file_type.is_dir() {
            out.extend(collect_parquet_paths_under_depth(&p, depth + 1)?);
        } else if file_type.is_file() && p.extension().is_some_and(|e| e == "parquet") {
            out.push(p);
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(feature = "delta")]
fn reject_symlinks_recursively(path: &Path, operation: &str) -> Result<(), PolarsError> {
    reject_symlinks_recursively_at_depth(path, operation, 0)
}

#[cfg(feature = "delta")]
fn reject_symlinks_recursively_at_depth(
    path: &Path,
    operation: &str,
    depth: u32,
) -> Result<(), PolarsError> {
    if depth > PARQUET_WALK_MAX_DEPTH {
        return Err(PolarsError::InvalidOperation(
            format!("{operation}: Delta log directory depth exceeds {PARQUET_WALK_MAX_DEPTH}")
                .into(),
        ));
    }

    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(PolarsError::ComputeError(
                format!("{operation}: inspect {}: {error}", path.display()).into(),
            ));
        }
    };

    if metadata.file_type().is_symlink() {
        return Err(PolarsError::InvalidOperation(
            format!(
                "{operation}: Delta log contains a symbolic link: {}",
                path.display()
            )
            .into(),
        ));
    }
    if !metadata.is_dir() {
        return Ok(());
    }

    let entries = std::fs::read_dir(path).map_err(|error| {
        PolarsError::ComputeError(format!("{operation}: read {}: {error}", path.display()).into())
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            PolarsError::ComputeError(format!("{operation}: read log entry: {error}").into())
        })?;
        reject_symlinks_recursively_at_depth(&entry.path(), operation, depth + 1)?;
    }
    Ok(())
}

#[cfg(feature = "delta")]
fn delta_field_physical_name(field: &deltalake::kernel::StructField) -> &str {
    use deltalake::kernel::MetadataValue;

    match field.metadata.get("delta.columnMapping.physicalName") {
        Some(MetadataValue::String(name)) => name,
        _ => &field.name,
    }
}

#[cfg(feature = "delta")]
fn delta_field_uses_column_mapping(field: &deltalake::kernel::StructField) -> bool {
    field
        .metadata
        .contains_key("delta.columnMapping.physicalName")
        || field.metadata.contains_key("delta.columnMapping.id")
        || delta_type_uses_column_mapping(&field.data_type)
}

#[cfg(feature = "delta")]
fn delta_type_uses_column_mapping(data_type: &deltalake::kernel::DataType) -> bool {
    use deltalake::kernel::DataType;

    match data_type {
        DataType::Struct(fields) | DataType::Variant(fields) => {
            fields.fields().any(delta_field_uses_column_mapping)
        }
        DataType::Array(array) => delta_type_uses_column_mapping(&array.element_type),
        DataType::Map(map) => {
            delta_type_uses_column_mapping(&map.key_type)
                || delta_type_uses_column_mapping(&map.value_type)
        }
        DataType::Primitive(_) => false,
    }
}

#[cfg(feature = "delta")]
fn delta_schema_uses_column_mapping(schema: &deltalake::kernel::StructType) -> bool {
    schema.fields().any(delta_field_uses_column_mapping)
}

#[cfg(feature = "delta")]
fn align_delta_expr_to_schema_type(
    expr: polars::prelude::Expr,
    source_dtype: &polars::prelude::DataType,
    target_type: &deltalake::kernel::DataType,
    case_insensitive_fields: bool,
) -> Result<polars::prelude::Expr, PolarsError> {
    use deltalake::kernel::DataType as DeltaDataType;
    use polars::prelude::{DataType, NULL, as_struct, col, lit, when};

    match (source_dtype, target_type) {
        (DataType::Struct(source_fields), DeltaDataType::Struct(target_fields))
        | (DataType::Struct(source_fields), DeltaDataType::Variant(target_fields)) => {
            let fields = target_fields
                .fields()
                .map(|target_field| {
                    let physical_name = delta_field_physical_name(target_field);
                    let target_dtype = delta_schema_type_to_polars(&target_field.data_type)?;
                    let field_expr = if let Some(source_field) =
                        source_fields.iter().find(|field| {
                            if case_insensitive_fields {
                                delta_names_equal(field.name().as_str(), physical_name)
                            } else {
                                field.name() == physical_name
                            }
                        }) {
                        align_delta_expr_to_schema_type(
                            expr.clone()
                                .struct_()
                                .field_by_name(source_field.name().as_str()),
                            source_field.dtype(),
                            &target_field.data_type,
                            case_insensitive_fields,
                        )?
                    } else {
                        lit(NULL).strict_cast(target_dtype)
                    };
                    Ok(field_expr.alias(target_field.name.as_str()))
                })
                .collect::<Result<Vec<_>, PolarsError>>()?;
            let aligned = as_struct(fields);
            let target_dtype = delta_schema_type_to_polars(target_type)?;
            Ok(when(expr.clone().is_null())
                .then(lit(NULL).strict_cast(target_dtype))
                .otherwise(aligned))
        }
        (DataType::List(source_inner), DeltaDataType::Array(target_array)) => {
            let item_expr = align_delta_expr_to_schema_type(
                col(""),
                source_inner,
                &target_array.element_type,
                case_insensitive_fields,
            )?;
            Ok(expr.list().eval(item_expr))
        }
        (DataType::List(source_inner), DeltaDataType::Map(target_map)) => {
            let DataType::Struct(source_fields) = source_inner.as_ref() else {
                return Err(PolarsError::InvalidOperation(
                    format!("cannot align Delta MAP from physical type {source_dtype:?}").into(),
                ));
            };
            let source_key = source_fields.iter().find(|field| field.name() == "key");
            let source_value = source_fields.iter().find(|field| field.name() == "value");
            let (Some(source_key), Some(source_value)) = (source_key, source_value) else {
                return Err(PolarsError::InvalidOperation(
                    format!("cannot align Delta MAP from physical type {source_dtype:?}").into(),
                ));
            };
            let entry_expr = as_struct(vec![
                col("")
                    .struct_()
                    .field_by_name("key")
                    .strict_cast(delta_schema_type_to_polars(&target_map.key_type)?)
                    .alias("key"),
                align_delta_expr_to_schema_type(
                    col("").struct_().field_by_name("value"),
                    source_value.dtype(),
                    &target_map.value_type,
                    case_insensitive_fields,
                )?
                .alias("value"),
            ]);
            let _ = source_key;
            Ok(expr.list().eval(entry_expr))
        }
        _ => Ok(expr.strict_cast(delta_schema_type_to_polars(target_type)?)),
    }
}

#[cfg(feature = "delta")]
fn delta_names_equal(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(feature = "delta")]
fn create_partition_dir_under_table_root(
    table_root: &Path,
    relative_parent: &Path,
) -> Result<std::path::PathBuf, PolarsError> {
    use std::path::Component;

    std::fs::create_dir_all(table_root).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create table root: {e}").into())
    })?;
    let canonical_root = table_root.canonicalize().map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: canonicalize table root: {e}").into())
    })?;
    if !canonical_root.is_dir() {
        return Err(PolarsError::InvalidOperation(
            format!(
                "write_delta: table root is not a directory: {}",
                canonical_root.display()
            )
            .into(),
        ));
    }

    let mut destination = table_root.to_path_buf();
    for component in relative_parent.components() {
        let Component::Normal(name) = component else {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "write_delta: invalid partition directory: {}",
                    relative_parent.display()
                )
                .into(),
            ));
        };
        destination.push(name);
        match std::fs::symlink_metadata(&destination) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "write_delta: partition directory is a symbolic link: {}",
                            destination.display()
                        )
                        .into(),
                    ));
                }
                if !metadata.is_dir() {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "write_delta: partition path is not a directory: {}",
                            destination.display()
                        )
                        .into(),
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::create_dir(&destination) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(PolarsError::ComputeError(
                            format!("write_delta: create partition directory: {error}").into(),
                        ));
                    }
                }
                let metadata = std::fs::symlink_metadata(&destination).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: inspect partition directory: {e}").into(),
                    )
                })?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "write_delta: unsafe partition directory: {}",
                            destination.display()
                        )
                        .into(),
                    ));
                }
            }
            Err(error) => {
                return Err(PolarsError::ComputeError(
                    format!("write_delta: inspect partition directory: {error}").into(),
                ));
            }
        }

        let canonical_destination = destination.canonicalize().map_err(|e| {
            PolarsError::ComputeError(
                format!("write_delta: canonicalize partition directory: {e}").into(),
            )
        })?;
        if !canonical_destination.starts_with(&canonical_root) {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "write_delta: partition directory escapes table root: {}",
                    destination.display()
                )
                .into(),
            ));
        }
    }
    Ok(destination)
}

/// Read a Delta table at the given path, optionally at a specific version (time travel).
/// If the path is not a Delta table (e.g. directory with parquet written by save()), falls back to scanning parquet files under the path.
#[cfg(feature = "delta")]
pub fn read_delta_with_version(
    path: impl AsRef<Path>,
    version: Option<i64>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use deltalake::DeltaTableBuilder;
    use tokio::runtime::Runtime;

    let normalized = normalize_delta_path(path.as_ref())?;
    let path = normalized.as_path();

    let table_result = (|| {
        let table_uri = path_to_table_uri(path)?;
        let table_root = uri_to_parquet_path(&table_uri)?;
        reject_symlinks_recursively(&table_root.join("_delta_log"), "read_delta")?;
        let rt = Runtime::new().map_err(|e| {
            PolarsError::ComputeError(format!("read_delta: failed to create runtime: {}", e).into())
        })?;
        let url = Url::parse(table_uri.as_str()).map_err(|e| {
            PolarsError::ComputeError(format!("read_delta: invalid table URI: {}", e).into())
        })?;
        let table = rt.block_on(async {
            let builder = DeltaTableBuilder::from_url(url.clone()).map_err(
                |e: deltalake::DeltaTableError| {
                    PolarsError::ComputeError(format!("read_delta: {}", e).into())
                },
            )?;
            let result = if let Some(v) = version {
                let version = u64::try_from(v).map_err(|_| {
                    PolarsError::ComputeError(format!("read_delta: invalid version {v}").into())
                })?;
                builder.with_version(version).load().await
            } else {
                builder.load().await
            };
            result.map_err(|e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("read_delta: {}", e).into())
            })
        })?;
        let snapshot = table.snapshot().map_err(|e: deltalake::DeltaTableError| {
            PolarsError::ComputeError(format!("read_delta: snapshot: {e}").into())
        })?;
        use futures::TryStreamExt;
        let eager_snapshot = snapshot.snapshot();
        let log_store = table.log_store();
        let files = rt
            .block_on(async {
                eager_snapshot
                    .file_views(log_store.as_ref(), None)
                    .try_fold(
                        Vec::new(),
                        |mut files, view: deltalake::kernel::LogicalFileView| {
                            // Keep partition values from the Add action. File paths are not required
                            // to use Hive-style directory names in a Delta table.
                            #[allow(deprecated)]
                            let add = view.add_action();
                            // Delta paths use `/`; also recognize the host separator so Windows
                            // traversal is rejected without treating Unix backslashes as separators.
                            if add
                                .path
                                .split(['/', std::path::MAIN_SEPARATOR])
                                .any(|part| part == "..")
                            {
                                return futures::future::ready(Err(
                                    deltalake::DeltaTableError::Generic(format!(
                                        "read_delta: parquet URI '{}' escapes table root '{}'",
                                        add.path, table_uri
                                    )),
                                ));
                            }
                            let file_path = deltalake::Path::parse(&add.path)
                                .unwrap_or_else(|_| deltalake::Path::from(add.path.as_str()));
                            files.push((log_store.to_uri(&file_path), add.partition_values));
                            futures::future::ready(Ok(files))
                        },
                    )
                    .await
            })
            .map_err(|e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("read_delta: file_views: {e}").into())
            })?;
        let partition_columns = snapshot.metadata().partition_columns().to_vec();
        let schema = snapshot.metadata().parse_schema().map_err(|e| {
            PolarsError::ComputeError(format!("read_delta: parse schema: {e}").into())
        })?;
        let partition_types = schema
            .fields()
            .filter(|field| partition_columns.iter().any(|name| name == &field.name))
            .map(|field| {
                serde_json::to_value(&field.data_type)
                    .map(|data_type| (field.name.clone(), data_type))
                    .map_err(|e| {
                        PolarsError::ComputeError(
                            format!("read_delta: serialize partition type: {e}").into(),
                        )
                    })
            })
            .collect::<Result<std::collections::HashMap<_, _>, _>>()?;
        Ok::<_, PolarsError>((files, partition_columns, partition_types, schema, table_uri))
    })();

    match table_result {
        Ok((files, partition_columns, partition_types, schema, table_uri)) => {
            if files.is_empty() {
                let columns = schema
                    .fields()
                    .map(|field| {
                        let dtype = delta_schema_type_to_polars(&field.data_type)?;
                        Ok(
                            polars::prelude::Series::new_empty(field.name.clone().into(), &dtype)
                                .into(),
                        )
                    })
                    .collect::<Result<Vec<polars::prelude::Column>, PolarsError>>()?;
                let pl_df = polars::prelude::DataFrame::new(0, columns)?;
                return Ok(DataFrame::from_polars_with_options(pl_df, case_sensitive));
            }
            let table_root = uri_to_parquet_path(&table_uri)?;
            let table_root_canonical = table_root.canonicalize().map_err(|e| {
                PolarsError::ComputeError(
                    format!("read_delta: canonicalize table root: {e}").into(),
                )
            })?;
            let mut scans = Vec::with_capacity(files.len());
            for (uri, _) in &files {
                let parquet_path = confine_parquet_uri_to_table_root(uri, &table_root_canonical)?;
                let pl_path = PlRefPath::try_from_path(&parquet_path).map_err(|e| {
                    PolarsError::ComputeError(format!("read_delta scan_parquet path: {e}").into())
                })?;
                let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
                let source_schema = lf.clone().collect_schema()?;
                scans.push((lf, source_schema));
            }
            let mut row_index_name = "__sparkless_delta_row_index".to_string();
            while scans
                .iter()
                .any(|(_, source_schema)| source_schema.get(row_index_name.as_str()).is_some())
            {
                row_index_name.push('_');
            }
            let mut lfs: Vec<LazyFrame> = Vec::with_capacity(files.len());
            for ((uri, partition_values), (lf, source_schema)) in files.iter().zip(scans) {
                // Keep an input column in the projection's execution context. Delta partition
                // values are scalar expressions, and selecting only those expressions from a
                // Parquet file would otherwise produce one row per file rather than one row per
                // physical record (notably for tables with no non-partition columns).
                let lf = lf.with_row_index(row_index_name.clone(), None);
                let mut partition_exprs =
                    std::collections::HashMap::with_capacity(partition_columns.len());
                for name in &partition_columns {
                    let Some(data_type) = partition_types.get(name) else {
                        return Err(PolarsError::ComputeError(
                            format!(
                                "read_delta: partition column '{name}' is missing from the schema"
                            )
                            .into(),
                        ));
                    };
                    let dtype = delta_partition_type_to_polars(data_type)?;
                    let value = partition_values
                        .get(name)
                        .ok_or_else(|| {
                            PolarsError::ComputeError(
                                format!(
                                    "read_delta: missing partition value for '{name}' in {uri}"
                                )
                                .into(),
                            )
                        })?
                        .clone()
                        .filter(|value| !value.is_empty());
                    let value_expr = match value {
                        Some(value)
                            if matches!(
                                data_type.as_str(),
                                Some("timestamp" | "timestamp_ntz")
                            ) =>
                        {
                            let micros = parse_delta_timestamp_partition_value(&value)?;
                            polars::prelude::lit(micros)
                        }
                        Some(value) if data_type.as_str() == Some("boolean") => {
                            let parsed = value.parse::<bool>().map_err(|_| {
                                PolarsError::ComputeError(
                                    format!(
                                        "read_delta: invalid boolean partition value: {value:?}"
                                    )
                                    .into(),
                                )
                            })?;
                            polars::prelude::lit(parsed)
                        }
                        Some(value) => polars::prelude::lit(value),
                        None => polars::prelude::lit(polars::prelude::NULL),
                    };
                    partition_exprs.insert(name.clone(), value_expr.cast(dtype));
                }

                let mut aligned_exprs = Vec::new();
                for field in schema.fields() {
                    let expr = if let Some(partition_expr) = partition_exprs.remove(&field.name) {
                        partition_expr
                    } else if let Some(source_field) =
                        source_schema.get(delta_field_physical_name(field))
                    {
                        align_delta_expr_to_schema_type(
                            polars::prelude::col(delta_field_physical_name(field)),
                            source_field,
                            &field.data_type,
                            false,
                        )?
                    } else {
                        polars::prelude::lit(polars::prelude::NULL)
                            .cast(delta_schema_type_to_polars(&field.data_type)?)
                    };
                    aligned_exprs.push(expr.alias(field.name.as_str()));
                }
                aligned_exprs.push(polars::prelude::col(row_index_name.as_str()));
                if let Some(missing_partition) = partition_exprs.keys().next() {
                    return Err(PolarsError::ComputeError(
                        format!(
                            "read_delta: partition column '{missing_partition}' is missing from the schema"
                        )
                        .into(),
                    ));
                }
                lfs.push(lf.select(aligned_exprs));
            }
            let combined = concat_lazy_frames_diagonal(lfs)?;
            let mut pl_df = combined.collect()?;
            pl_df.drop_in_place(row_index_name.as_str())?;
            Ok(DataFrame::from_polars_with_options(pl_df, case_sensitive))
        }
        Err(e) => {
            let err_msg = e.to_string();
            let is_not_delta =
                err_msg.contains("No files in log") || err_msg.contains("Not a Delta table");
            if version.is_none() && is_not_delta && path.exists() && path.is_dir() {
                let parquet_paths = collect_parquet_paths_under(path)?;
                if parquet_paths.is_empty() {
                    return Err(e);
                }
                let mut lfs: Vec<LazyFrame> = Vec::with_capacity(parquet_paths.len());
                for p in &parquet_paths {
                    let pl_path = PlRefPath::try_from_path(p).map_err(|e2| {
                        PolarsError::ComputeError(
                            format!("read_delta fallback scan_parquet path: {e2}").into(),
                        )
                    })?;
                    let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
                    lfs.push(lf);
                }
                let combined = concat_lazy_frames_diagonal(lfs)?;
                let pl_df = combined.collect()?;
                return Ok(DataFrame::from_polars_with_options(pl_df, case_sensitive));
            }
            Err(e)
        }
    }
}

#[cfg(feature = "delta")]
fn path_to_table_uri(path: &Path) -> Result<String, PolarsError> {
    let s = path.to_string_lossy();
    if s.starts_with("file://") {
        return Ok(s.to_string());
    }
    // Resolve to absolute path; support paths that don't exist yet (e.g. for write_delta).
    let abs_path = if path.exists() {
        path.canonicalize().map_err(|e| {
            PolarsError::ComputeError(format!("path_to_table_uri: canonicalize: {}", e).into())
        })?
    } else {
        let base = path.parent().unwrap_or(Path::new("."));
        let name = path.file_name().unwrap_or(std::ffi::OsStr::new("."));
        if base.exists() {
            base.canonicalize()
                .map_err(|e| {
                    PolarsError::ComputeError(
                        format!("path_to_table_uri: canonicalize parent: {}", e).into(),
                    )
                })?
                .join(name)
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| Path::new(".").to_path_buf())
                .join(path)
        }
    };
    Url::from_file_path(&abs_path)
        .map(|url| url.to_string())
        .map_err(|_| {
            PolarsError::ComputeError(
                format!(
                    "path_to_table_uri: invalid path for file URL: {}",
                    abs_path.display()
                )
                .into(),
            )
        })
}

#[cfg(feature = "delta")]
fn normalize_delta_path(path: &Path) -> Result<std::path::PathBuf, PolarsError> {
    let raw = path.to_string_lossy();
    if raw.starts_with("file:") {
        let url = Url::parse(&raw).map_err(|error| {
            PolarsError::InvalidOperation(format!("invalid Delta file URL: {error}").into())
        })?;
        return url
            .to_file_path()
            .map_err(|()| PolarsError::InvalidOperation("invalid Delta file URL path".into()));
    }
    Ok(path.to_path_buf())
}

#[cfg(feature = "delta")]
fn confine_parquet_uri_to_table_root(
    uri: &str,
    table_root: &Path,
) -> Result<std::path::PathBuf, PolarsError> {
    // Delta-rs may prefix absolute file URIs with the table path, e.g.
    // `/data/table/file:/etc/passwd`. Extract and confine the embedded URI.
    if let Some(pos) = uri.find("file:") {
        if pos > 0 {
            return confine_parquet_uri_to_table_root(&uri[pos..], table_root);
        }
    }
    let table_canonical = table_root.canonicalize().map_err(|e| {
        PolarsError::ComputeError(format!("read_delta: canonicalize table root: {e}").into())
    })?;
    // Delta file URIs append the encoded Add.path directly to the root URI. Preserve percent
    // escapes in the relative path because they are literal characters in the on-disk partition
    // directory (for example, a slash in a value is stored as `%2F`). Decode only the root URI.
    let parquet_path = match raw_file_uri_relative_path(uri, &table_canonical) {
        Some(relative_path) => relative_path,
        None => uri_to_parquet_path(uri)?,
    };
    let resolved = if parquet_path.is_absolute() {
        parquet_path
    } else {
        table_canonical.join(parquet_path)
    };
    let canonical = canonicalize_path_for_confinement(&resolved, &table_canonical)?;
    if !canonical.starts_with(&table_canonical) {
        return Err(PolarsError::ComputeError(
            format!(
                "read_delta: parquet URI '{}' escapes table root '{}'",
                canonical.display(),
                table_canonical.display()
            )
            .into(),
        ));
    }
    Ok(canonical)
}

#[cfg(feature = "delta")]
fn raw_file_uri_relative_path(uri: &str, table_root: &Path) -> Option<std::path::PathBuf> {
    let root_uri = Url::from_directory_path(table_root).ok()?.to_string();
    let relative_after_root = |root: &str| {
        let prefix = root.strip_suffix('/').unwrap_or(root);
        uri.strip_prefix(prefix)?.strip_prefix('/')
    };

    // Delta-rs preserves escapes in the suffix, but its local file log store strips the
    // `file://` scheme and returns a bare filesystem path. Match both forms against the URL-
    // encoded root so spaces and other escaped characters in the table path still work.
    let relative = relative_after_root(&root_uri).or_else(|| {
        #[cfg(windows)]
        let bare_root = root_uri.strip_prefix("file:///")?;
        #[cfg(not(windows))]
        let bare_root = root_uri.strip_prefix("file://")?;
        relative_after_root(bare_root)
    })?;
    if relative.is_empty() || relative.starts_with('/') {
        return None;
    }
    Some(std::path::PathBuf::from(relative))
}

/// Resolve `..` / `.` by canonicalizing the nearest existing ancestor (same idea as path_security).
#[cfg(feature = "delta")]
fn canonicalize_path_for_confinement(
    resolved: &Path,
    fallback_base: &Path,
) -> Result<std::path::PathBuf, PolarsError> {
    if resolved.exists() {
        return resolved.canonicalize().map_err(|e| {
            PolarsError::ComputeError(format!("read_delta: canonicalize parquet path: {e}").into())
        });
    }
    let parent = resolved.parent().unwrap_or(fallback_base);
    let file_name = resolved
        .file_name()
        .ok_or_else(|| PolarsError::ComputeError("read_delta: invalid parquet path".into()))?;
    if parent.exists() {
        let canonical_parent = parent.canonicalize().map_err(|e| {
            PolarsError::ComputeError(format!("read_delta: canonicalize parent: {e}").into())
        })?;
        Ok(canonical_parent.join(file_name))
    } else {
        Ok(resolved.to_path_buf())
    }
}

#[cfg(feature = "delta")]
fn decode_percent_path(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8()
        .map(|cow| cow.into_owned())
        .unwrap_or_else(|_| s.to_string())
}

#[cfg(feature = "delta")]
fn uri_to_parquet_path(uri: &str) -> Result<std::path::PathBuf, PolarsError> {
    let file_uri = uri
        .strip_prefix("file://")
        .or_else(|| uri.strip_prefix("file:/"));
    if let Some(stripped) = file_uri {
        #[cfg(target_os = "windows")]
        {
            let s = decode_percent_path(stripped.trim_start_matches('/')).replace('/', "\\");
            return Ok(std::path::PathBuf::from(s));
        }
        #[cfg(not(target_os = "windows"))]
        {
            let s = decode_percent_path(stripped.trim_start_matches('/'));
            return Ok(std::path::PathBuf::from(format!("/{s}")));
        }
    }
    // deltalake may return bare absolute paths (e.g. /private/var/.../file.parquet).
    if uri.starts_with('/')
        || (cfg!(target_os = "windows") && uri.len() >= 2 && uri.chars().nth(1) == Some(':'))
    {
        return Ok(std::path::PathBuf::from(decode_percent_path(uri)));
    }
    // Relative paths within the table directory (common in Delta add actions).
    if !uri.contains("://") && !uri.starts_with("file:") {
        return Ok(std::path::PathBuf::from(decode_percent_path(uri)));
    }
    Err(PolarsError::ComputeError(
        format!("read_delta: unsupported URI (local file only): {}", uri).into(),
    ))
}

/// Return a single-row DataFrame with Delta table metadata (DESCRIBE DETAIL parity).
/// Columns: format, name, id, description, location, numFiles, sizeInBytes, minReaderVersion,
/// minWriterVersion, partitionColumns, and optionally createdAt, lastModified, properties.
#[cfg(feature = "delta")]
pub fn describe_delta_detail(
    path: impl AsRef<Path>,
    table_display_name: Option<&str>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use deltalake::DeltaTableBuilder;
    use polars::prelude::*;
    use tokio::runtime::Runtime;

    let path = path.as_ref();
    let table_uri = path_to_table_uri(path)?;

    let rt = Runtime::new().map_err(|e| {
        PolarsError::ComputeError(format!("describe_delta_detail: runtime: {}", e).into())
    })?;

    let url = Url::parse(table_uri.as_str()).map_err(|e| {
        PolarsError::ComputeError(format!("describe_delta_detail: invalid URI: {}", e).into())
    })?;

    let table = rt.block_on(async {
        let builder =
            DeltaTableBuilder::from_url(url.clone()).map_err(|e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("describe_delta_detail: {}", e).into())
            })?;
        let t = builder
            .load()
            .await
            .map_err(|e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("describe_delta_detail: load: {}", e).into())
            })?;
        Ok::<_, PolarsError>(t)
    })?;

    let snapshot = table.snapshot().map_err(|e: deltalake::DeltaTableError| {
        PolarsError::ComputeError(format!("describe_delta_detail: snapshot: {}", e).into())
    })?;

    let meta = snapshot.metadata();
    let protocol = snapshot.protocol();
    let log_data = snapshot.log_data();
    let num_files = log_data.num_files();

    let name = table_display_name
        .map(String::from)
        .or_else(|| meta.name().map(String::from))
        .unwrap_or_else(|| path.display().to_string());
    let id = meta.id().to_string();
    let description = meta.description().map(String::from);
    let location = table_uri.clone();
    let format = "delta".to_string();
    let min_reader_version = protocol.min_reader_version() as i64;
    let min_writer_version = protocol.min_writer_version() as i64;
    let partition_columns: Vec<String> = meta.partition_columns().to_vec();
    let partition_columns_json =
        serde_json::to_string(&partition_columns).unwrap_or_else(|_| "[]".to_string());

    // sizeInBytes: sum of file sizes from add actions (LogicalFileView::size())
    let size_in_bytes: i64 = log_data.iter().map(|view| view.size()).sum();

    let created_at = meta.created_time();
    let version = snapshot.version();

    let df = polars::prelude::DataFrame::new_infer_height(vec![
        Series::new("format".into(), [format]).into(),
        Series::new("name".into(), [name]).into(),
        Series::new("id".into(), [id]).into(),
        Series::new("description".into(), [description]).into(),
        Series::new("location".into(), [location]).into(),
        Series::new("numFiles".into(), [num_files as i64]).into(),
        Series::new("sizeInBytes".into(), [size_in_bytes]).into(),
        Series::new("minReaderVersion".into(), [min_reader_version]).into(),
        Series::new("minWriterVersion".into(), [min_writer_version]).into(),
        Series::new("partitionColumns".into(), [partition_columns_json]).into(),
        Series::new("createdAt".into(), [created_at]).into(),
        Series::new("version".into(), [version]).into(),
    ])
    .map_err(|e| PolarsError::ComputeError(format!("describe_delta_detail: df: {}", e).into()))?;

    Ok(crate::dataframe::DataFrame::from_polars_with_options(
        df,
        case_sensitive,
    ))
}

#[cfg(feature = "delta")]
fn is_not_a_table(err: &deltalake::DeltaTableError) -> bool {
    matches!(err, deltalake::DeltaTableError::NotATable(_))
}

/// Write a single parquet part file under `path` and return its path.
#[cfg(feature = "delta")]
fn write_parquet_part(
    df: &polars::prelude::DataFrame,
    path: &Path,
    logical_map_paths: &[String],
) -> Result<std::path::PathBuf, PolarsError> {
    let part_id: u64 = rand::random();
    let parquet_path = path.join(format!("part-{part_id:016x}.parquet"));
    write_delta_parquet_file(df, &parquet_path, logical_map_paths)?;
    Ok(parquet_path)
}

#[cfg(feature = "delta")]
fn write_delta_parquet_file(
    df: &polars::prelude::DataFrame,
    parquet_path: &Path,
    logical_map_paths: &[String],
) -> Result<(), PolarsError> {
    use std::fs;

    let mut file = std::io::BufWriter::new(fs::File::create(parquet_path).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create parquet file: {}", e).into())
    })?);
    if logical_map_paths.is_empty() {
        let mut df_mut = df.clone();
        polars::prelude::ParquetWriter::new(&mut file)
            .finish(&mut df_mut)
            .map_err(|e| {
                PolarsError::ComputeError(format!("write_delta: parquet write: {}", e).into())
            })?;
        return Ok(());
    }

    write_delta_parquet_with_logical_maps(df, file, logical_map_paths)
}

#[cfg(feature = "delta")]
fn write_delta_parquet_with_logical_maps<W: std::io::Write + Send>(
    df: &polars::prelude::DataFrame,
    file: W,
    logical_map_paths: &[String],
) -> Result<(), PolarsError> {
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::Schema as ArrowSchema;
    use std::sync::Arc;

    let parsed_paths = logical_map_paths
        .iter()
        .map(|path| parse_delta_schema_path(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut rechunked = df.clone();
    rechunked.rechunk_mut();
    let mut batches = Vec::new();
    for batch in rechunked.iter_chunks(polars::prelude::CompatLevel::newest(), false) {
        let mut fields = Vec::with_capacity(batch.width());
        let mut arrays = Vec::<ArrayRef>::with_capacity(batch.width());
        for ((_, field), array) in batch.schema().iter().zip(batch.arrays()) {
            let field_paths = parsed_paths
                .iter()
                .filter_map(|path| match path.split_first() {
                    Some((DeltaSchemaPathSegment::Field(name), tail))
                        if delta_names_equal(name, field.name.as_str()) =>
                    {
                        Some(tail.to_vec())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let (field, array) =
                rewrite_arrow_field_array(field.clone(), array.clone(), &field_paths)?;
            let (arrow_field, array) = polars_arrow_to_arrow_rs(&field, array)?;
            fields.push(arrow_field);
            arrays.push(array);
        }

        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: build Arrow record batch: {e}").into())
        })?;
        batches.push(batch);
    }

    let schema = batches
        .first()
        .map(|batch| batch.schema().clone())
        .unwrap_or_else(|| Arc::new(ArrowSchema::empty()));
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create Arrow Parquet writer: {e}").into())
    })?;
    for batch in &batches {
        writer.write(batch).map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: write Arrow Parquet batch: {e}").into())
        })?;
    }
    writer.close().map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: finish Arrow Parquet file: {e}").into())
    })?;
    Ok(())
}

#[cfg(feature = "delta")]
fn polars_arrow_to_arrow_rs(
    field: &polars_arrow::datatypes::Field,
    array: Box<dyn polars_arrow::array::Array>,
) -> Result<(arrow_schema::Field, arrow_array::ArrayRef), PolarsError> {
    use arrow_array::{ffi, make_array};
    use arrow_schema::{Field, ffi::FFI_ArrowSchema};

    let ffi_field = polars_arrow::ffi::export_field_to_c(field);
    // Both crates implement the Arrow C Data Interface, whose C layouts are ABI-stable.
    let ffi_field: FFI_ArrowSchema = unsafe { std::mem::transmute(ffi_field) };
    let arrow_field = Field::try_from(&ffi_field).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: convert Arrow field: {e}").into())
    })?;

    let ffi_array = polars_arrow::ffi::export_array_to_c(array);
    // Transfer ownership through the Arrow C Data Interface so Arrow-rs can write MAP fields.
    let ffi_array: ffi::FFI_ArrowArray = unsafe { std::mem::transmute(ffi_array) };
    let data = unsafe { ffi::from_ffi(ffi_array, &ffi_field) }.map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: convert Arrow array: {e}").into())
    })?;
    Ok((arrow_field, make_array(data)))
}

#[cfg(feature = "delta")]
fn rewrite_arrow_field_array(
    mut field: polars_arrow::datatypes::Field,
    array: Box<dyn polars_arrow::array::Array>,
    paths: &[Vec<DeltaSchemaPathSegment>],
) -> Result<
    (
        polars_arrow::datatypes::Field,
        Box<dyn polars_arrow::array::Array>,
    ),
    PolarsError,
> {
    use polars_arrow::array::{Array, ListArray, MapArray, StructArray};
    use polars_arrow::datatypes::ArrowDataType;

    let exact_map = paths.iter().any(Vec::is_empty);
    if exact_map {
        let map_paths = paths
            .iter()
            .filter_map(|path| match path.split_first() {
                Some((DeltaSchemaPathSegment::MapKey, tail)) => {
                    let mut nested = vec![DeltaSchemaPathSegment::Field("key".into())];
                    nested.extend_from_slice(tail);
                    Some(nested)
                }
                Some((DeltaSchemaPathSegment::MapValue, tail)) => {
                    let mut nested = vec![DeltaSchemaPathSegment::Field("value".into())];
                    nested.extend_from_slice(tail);
                    Some(nested)
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        let (list_dtype, entries, offsets, validity) = match array.dtype() {
            ArrowDataType::List(_) => {
                let list = array
                    .as_any()
                    .downcast_ref::<ListArray<i32>>()
                    .ok_or_else(|| {
                        PolarsError::ComputeError("write_delta: invalid Arrow List array".into())
                    })?;
                list.clone().into_inner()
            }
            ArrowDataType::LargeList(_) => {
                let list = array
                    .as_any()
                    .downcast_ref::<ListArray<i64>>()
                    .ok_or_else(|| {
                        PolarsError::ComputeError(
                            "write_delta: invalid Arrow LargeList array".into(),
                        )
                    })?;
                let (dtype, values, offsets, validity) = list.clone().into_inner();
                let offsets = polars_arrow::offset::OffsetsBuffer::<i32>::try_from(&offsets)
                    .map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write_delta: map offsets exceed Parquet limits: {e}").into(),
                        )
                    })?;
                (dtype, values, offsets, validity)
            }
            other => {
                return Err(PolarsError::InvalidOperation(
                    format!("write_delta: logical MapType requires a list of key/value structs, got {other:?}").into(),
                ));
            }
        };
        let entry_field = match list_dtype {
            ArrowDataType::List(field) | ArrowDataType::LargeList(field) => *field,
            _ => unreachable!("the array type was checked above"),
        };
        let (_entry_field, entries) = rewrite_arrow_field_array(entry_field, entries, &map_paths)?;
        let entry_struct = entries
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                PolarsError::InvalidOperation(
                    "write_delta: logical MapType requires key/value struct entries".into(),
                )
            })?;
        let entry_fields = entry_struct.fields();
        if entry_fields.len() != 2 {
            return Err(PolarsError::InvalidOperation(
                "write_delta: logical MapType entries must contain exactly key and value fields"
                    .into(),
            ));
        }
        let key_index = entry_fields
            .iter()
            .position(|field| field.name.as_str() == "key");
        let value_index = entry_fields
            .iter()
            .position(|field| field.name.as_str() == "value");
        let (Some(key_index), Some(value_index)) = (key_index, value_index) else {
            return Err(PolarsError::InvalidOperation(
                "write_delta: logical MapType entries must contain key and value fields".into(),
            ));
        };
        if entry_struct.null_count() != 0 {
            return Err(PolarsError::InvalidOperation(
                "write_delta: logical MapType entries cannot be null".into(),
            ));
        }
        let key_values = entry_struct.values()[key_index].clone();
        let value_values = entry_struct.values()[value_index].clone();
        if key_values.null_count() != 0 {
            return Err(PolarsError::InvalidOperation(
                "write_delta: logical MapType keys cannot be null".into(),
            ));
        }

        let mut key_field = entry_fields[key_index].clone();
        let value_field = entry_fields[value_index].clone();
        if !value_field.is_nullable && value_values.null_count() != 0 {
            return Err(PolarsError::InvalidOperation(
                "write_delta: non-nullable logical MapType values contain nulls".into(),
            ));
        }
        key_field.is_nullable = false;
        let entry_dtype = ArrowDataType::Struct(vec![key_field.clone(), value_field.clone()]);
        let entries = StructArray::try_new(
            entry_dtype.clone(),
            entry_struct.len(),
            vec![key_values, value_values],
            entry_struct.validity().cloned(),
        )
        .map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: normalize map entries: {e}").into())
        })?;
        let entries_field =
            polars_arrow::datatypes::Field::new("entries".into(), entry_dtype, false);
        let map_dtype = ArrowDataType::Map(Box::new(entries_field.clone()), false);
        let map = MapArray::try_new(map_dtype.clone(), offsets, Box::new(entries), validity)
            .map_err(|e| {
                PolarsError::ComputeError(format!("write_delta: build Arrow Map array: {e}").into())
            })?;
        field.dtype = map_dtype;
        return Ok((field, Box::new(map)));
    }

    match array.dtype() {
        ArrowDataType::Struct(_) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    PolarsError::ComputeError("write_delta: invalid Arrow Struct array".into())
                })?;
            let mut fields = Vec::with_capacity(struct_array.fields().len());
            let mut values = Vec::with_capacity(struct_array.values().len());
            for (child_field, child_array) in
                struct_array.fields().iter().zip(struct_array.values())
            {
                let child_paths = paths
                    .iter()
                    .filter_map(|path| match path.split_first() {
                        Some((DeltaSchemaPathSegment::Field(name), tail))
                            if delta_names_equal(name, child_field.name.as_str()) =>
                        {
                            Some(tail.to_vec())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let (child_field, child_array) = if child_paths.is_empty() {
                    (child_field.clone(), child_array.clone())
                } else {
                    rewrite_arrow_field_array(
                        child_field.clone(),
                        child_array.clone(),
                        &child_paths,
                    )?
                };
                fields.push(child_field);
                values.push(child_array);
            }
            let dtype = ArrowDataType::Struct(fields);
            let updated = StructArray::try_new(
                dtype.clone(),
                struct_array.len(),
                values,
                struct_array.validity().cloned(),
            )
            .map_err(|e| {
                PolarsError::ComputeError(format!("write_delta: rewrite Arrow struct: {e}").into())
            })?;
            field.dtype = dtype;
            Ok((field, Box::new(updated)))
        }
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
            let child_paths = paths
                .iter()
                .filter_map(|path| match path.split_first() {
                    Some((DeltaSchemaPathSegment::ArrayElement, tail)) => Some(tail.to_vec()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if child_paths.is_empty() {
                return Ok((field, array));
            }
            match array.dtype() {
                ArrowDataType::List(_) => {
                    let list =
                        array
                            .as_any()
                            .downcast_ref::<ListArray<i32>>()
                            .ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "write_delta: invalid Arrow List array".into(),
                                )
                            })?;
                    let (dtype, values, offsets, validity) = list.clone().into_inner();
                    let child_field = match dtype {
                        ArrowDataType::List(child) => *child,
                        _ => unreachable!(),
                    };
                    let (child_field, values) =
                        rewrite_arrow_field_array(child_field, values, &child_paths)?;
                    let dtype = ArrowDataType::List(Box::new(child_field));
                    let updated =
                        ListArray::<i32>::try_new(dtype.clone(), offsets, values, validity)
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("write_delta: rewrite Arrow list: {e}").into(),
                                )
                            })?;
                    field.dtype = dtype;
                    Ok((field, Box::new(updated)))
                }
                ArrowDataType::LargeList(_) => {
                    let list =
                        array
                            .as_any()
                            .downcast_ref::<ListArray<i64>>()
                            .ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "write_delta: invalid Arrow LargeList array".into(),
                                )
                            })?;
                    let (dtype, values, offsets, validity) = list.clone().into_inner();
                    let child_field = match dtype {
                        ArrowDataType::LargeList(child) => *child,
                        _ => unreachable!(),
                    };
                    let (child_field, values) =
                        rewrite_arrow_field_array(child_field, values, &child_paths)?;
                    let dtype = ArrowDataType::LargeList(Box::new(child_field));
                    let updated =
                        ListArray::<i64>::try_new(dtype.clone(), offsets, values, validity)
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("write_delta: rewrite Arrow large list: {e}").into(),
                                )
                            })?;
                    field.dtype = dtype;
                    Ok((field, Box::new(updated)))
                }
                _ => unreachable!(),
            }
        }
        other
            if paths.iter().any(|path| {
                path.iter().any(|segment| {
                    matches!(
                        segment,
                        DeltaSchemaPathSegment::ArrayElement | DeltaSchemaPathSegment::Field(_)
                    )
                })
            }) =>
        {
            Err(PolarsError::InvalidOperation(
                format!("write_delta: logical map path cannot descend into Arrow type {other:?}")
                    .into(),
            ))
        }
        _ => Ok((field, array)),
    }
}

#[cfg(feature = "delta")]
fn delta_partition_field(
    df: &polars::prelude::DataFrame,
    name: &str,
) -> Result<deltalake::kernel::StructField, PolarsError> {
    use polars::prelude::DataType as PlDataType;

    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '=', '\0']) {
        return Err(PolarsError::InvalidOperation(
            format!("invalid Delta partition column name: {name:?}").into(),
        ));
    }

    let data_type = df.schema().get(name).ok_or_else(|| {
        PolarsError::ColumnNotFound(format!("partition column '{name}' was not found").into())
    })?;
    let delta_type = match data_type {
        PlDataType::String => serde_json::json!("string"),
        PlDataType::Boolean => serde_json::json!("boolean"),
        PlDataType::Int8 => serde_json::json!("byte"),
        PlDataType::Int16 => serde_json::json!("short"),
        PlDataType::Int32 | PlDataType::UInt8 | PlDataType::UInt16 | PlDataType::UInt32 => {
            serde_json::json!("integer")
        }
        PlDataType::Int64 | PlDataType::UInt64 => serde_json::json!("long"),
        PlDataType::Float32 => serde_json::json!("float"),
        PlDataType::Float64 => serde_json::json!("double"),
        PlDataType::Binary => serde_json::json!("binary"),
        PlDataType::Date => serde_json::json!("date"),
        PlDataType::Datetime(_, time_zone) => serde_json::json!(if time_zone.is_some() {
            "timestamp"
        } else {
            "timestamp_ntz"
        }),
        PlDataType::Decimal(precision, scale) => {
            serde_json::json!(format!("decimal({},{})", precision, scale))
        }
        _ => {
            return Err(PolarsError::InvalidOperation(
                format!("unsupported Delta partition type for column '{name}': {data_type}").into(),
            ));
        }
    };

    serde_json::from_value(serde_json::json!({
        "name": name,
        "type": delta_type,
        "nullable": true,
        "metadata": {},
    }))
    .map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: invalid partition schema: {e}").into())
    })
}

#[cfg(feature = "delta")]
async fn write_partitioned_parquet(
    df: &polars::prelude::DataFrame,
    path: &Path,
    partition_columns: &[String],
    mode: crate::dataframe::WriteMode,
    logical_map_paths: &[String],
) -> Result<(), PolarsError> {
    let frame = DataFrame::from_polars(df.clone());
    frame
        .write()
        .mode(mode)
        .format(crate::dataframe::WriteFormat::Parquet)
        .partition_by(partition_columns.iter().cloned())
        .save(path)?;

    if !logical_map_paths.is_empty() {
        use std::fs;

        let parquet_paths = collect_parquet_paths_under(path)?;
        let logical_map_paths = logical_map_paths.to_vec();
        tokio::task::spawn_blocking(move || {
            for parquet_path in parquet_paths {
                let pl_path = PlRefPath::try_from_path(&parquet_path).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: staged partition Parquet path: {e}").into(),
                    )
                })?;
                let partition_frame =
                    LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?.collect()?;
                let rewritten_path = parquet_path.with_file_name(format!(
                    ".sparkless_delta_map_{:016x}.parquet",
                    rand::random::<u64>()
                ));
                write_delta_parquet_file(&partition_frame, &rewritten_path, &logical_map_paths)?;
                fs::rename(&rewritten_path, &parquet_path).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: replace staged partition Parquet file: {e}").into(),
                    )
                })?;
            }
            Ok::<(), PolarsError>(())
        })
        .await
        .map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: partition rewrite task: {e}").into())
        })??;
    }
    Ok(())
}

#[cfg(feature = "delta")]
async fn write_delta_bootstrap_partitioned(
    df: &polars::prelude::DataFrame,
    path: &Path,
    table_uri: &str,
    partition_columns: &[String],
    logical_map_paths: &[String],
) -> Result<(), PolarsError> {
    use deltalake::operations::create::CreateBuilder;
    use std::collections::HashSet;

    // When appending to a plain path, CreateBuilder adopts every Parquet file it finds.
    // Files without the requested partition directories have no partition values in their
    // Add actions, and readers then cannot reconstruct the table's partition columns.
    // Reject that mixed layout before writing any new files instead of creating an unreadable
    // Delta table.
    let existing_files = collect_parquet_paths_under(path)?;
    let requested_partitions: HashSet<String> = partition_columns.iter().cloned().collect();
    for file in &existing_files {
        let action = parquet_file_to_add_action(file, path).map_err(|e| {
            PolarsError::ComputeError(
                format!("write_delta: inspect existing partition file: {e}").into(),
            )
        })?;
        let actual_partitions = match action {
            deltalake::kernel::Action::Add(add) => {
                add.partition_values.into_keys().collect::<HashSet<_>>()
            }
            _ => unreachable!("parquet_file_to_add_action always returns an Add action"),
        };
        if actual_partitions != requested_partitions {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "cannot append partitioned Delta data to '{}': existing Parquet file '{}' is not partitioned by {:?}; use overwrite or a path with matching partition directories",
                    path.display(),
                    file.display(),
                    partition_columns
                )
                .into(),
            ));
        }
    }

    // Include schemas from both the existing Parquet files and the incoming frame. Otherwise
    // CreateBuilder's explicit schema would drop columns already present in the plain path.
    // Materialize outside the async runtime because Polars collection may create its own pool.
    let schema_files = existing_files.clone();
    let incoming_for_schema = df.clone();
    let schema_frame = tokio::task::spawn_blocking(move || {
        let mut frames = Vec::with_capacity(schema_files.len() + 1);
        for file in schema_files {
            let pl_path = PlRefPath::try_from_path(&file).map_err(|e| {
                PolarsError::ComputeError(
                    format!("write_delta: existing bootstrap Parquet path: {e}").into(),
                )
            })?;
            frames.push(LazyFrame::scan_parquet(
                pl_path,
                ScanArgsParquet::default(),
            )?);
        }
        frames.push(incoming_for_schema.lazy());
        if frames.len() == 1 {
            frames.pop().expect("one incoming schema frame").collect()
        } else {
            concat_lazy_frames_diagonal(frames)?.collect()
        }
    })
    .await
    .map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: merge bootstrap schemas: {e}").into())
    })??;
    let schema = delta_schema_from_frame(&schema_frame, logical_map_paths).await?;

    // Stage new files first, then move them into the table only after checking every destination
    // partition directory for symlinks and table-root confinement.
    write_partitioned_delta_parts(df, path, partition_columns, logical_map_paths).await?;

    let files = collect_parquet_paths_under(path)?;
    let actions = files
        .iter()
        .map(|file| parquet_file_to_add_action(file, path))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: add bootstrap partition: {e}").into())
        })?;

    CreateBuilder::new()
        .with_location(table_uri)
        .with_columns(schema.fields().cloned())
        .with_partition_columns(partition_columns.iter().cloned())
        .with_actions(actions)
        .await
        .map_err(|e: deltalake::DeltaTableError| {
            PolarsError::ComputeError(format!("write_delta: create partitioned table: {e}").into())
        })?;
    Ok(())
}

#[cfg(feature = "delta")]
async fn write_partitioned_delta_parts(
    df: &polars::prelude::DataFrame,
    table_root: &Path,
    partition_columns: &[String],
    logical_map_paths: &[String],
) -> Result<Vec<deltalake::kernel::Action>, PolarsError> {
    use std::fs;

    fs::create_dir_all(table_root).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create table root: {e}").into())
    })?;
    let staging = table_root.join(format!(
        ".sparkless_delta_write_{:016x}",
        rand::random::<u64>()
    ));
    let result = async {
        fs::create_dir(&staging).map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: create staging directory: {e}").into())
        })?;
        write_partitioned_parquet(
            df,
            &staging,
            partition_columns,
            crate::dataframe::WriteMode::Append,
            logical_map_paths,
        )
        .await?;
        let staged_files = collect_parquet_paths_under(&staging)?;
        let mut moves = Vec::with_capacity(staged_files.len());
        for staged_file in staged_files {
            let relative_path = staged_file.strip_prefix(&staging).map_err(|e| {
                PolarsError::ComputeError(
                    format!("write_delta: partition staging path: {e}").into(),
                )
            })?;
            let relative_parent = relative_path.parent().unwrap_or(Path::new(""));
            let destination_dir =
                create_partition_dir_under_table_root(table_root, relative_parent)?;
            let destination =
                destination_dir.join(format!("part-{:016x}.parquet", rand::random::<u64>()));
            moves.push((staged_file, destination));
        }

        let mut actions = Vec::with_capacity(moves.len());
        for (staged_file, destination) in moves {
            fs::rename(&staged_file, &destination).map_err(|e| {
                PolarsError::ComputeError(format!("write_delta: move partition file: {e}").into())
            })?;
            actions.push(
                parquet_file_to_add_action(&destination, table_root).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: add partition action: {e}").into(),
                    )
                })?,
            );
        }
        Ok(actions)
    }
    .await;
    let _ = fs::remove_dir_all(&staging);
    result
}

#[cfg(feature = "delta")]
#[derive(Clone, Debug, PartialEq, Eq)]
enum DeltaSchemaPathSegment {
    Field(String),
    ArrayElement,
    MapKey,
    MapValue,
}

#[cfg(feature = "delta")]
fn parse_delta_schema_path(path: &str) -> Result<Vec<DeltaSchemaPathSegment>, PolarsError> {
    let bytes = path.as_bytes();
    let mut offset = 0;
    let mut segments = Vec::new();
    while offset < bytes.len() {
        match bytes[offset] {
            b'"' => {
                let start = offset;
                offset += 1;
                let mut escaped = false;
                while offset < bytes.len() {
                    let byte = bytes[offset];
                    offset += 1;
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        break;
                    }
                }
                let field = serde_json::from_str::<String>(&path[start..offset]).map_err(|e| {
                    PolarsError::InvalidOperation(
                        format!("write_delta: invalid logical map path {path:?}: {e}").into(),
                    )
                })?;
                segments.push(DeltaSchemaPathSegment::Field(field));
            }
            b'.' => offset += 1,
            b'[' if bytes.get(offset + 1) == Some(&b']') => {
                offset += 2;
                segments.push(DeltaSchemaPathSegment::ArrayElement);
            }
            b'{' if path[offset..].starts_with("{key}") => {
                offset += 5;
                segments.push(DeltaSchemaPathSegment::MapKey);
            }
            b'{' if path[offset..].starts_with("{value}") => {
                offset += 7;
                segments.push(DeltaSchemaPathSegment::MapValue);
            }
            _ => {
                return Err(PolarsError::InvalidOperation(
                    format!("write_delta: invalid logical map path {path:?}").into(),
                ));
            }
        }
    }
    Ok(segments)
}

#[cfg(feature = "delta")]
fn rewrite_delta_struct_for_logical_maps(
    schema: deltalake::kernel::StructType,
    paths: &[Vec<DeltaSchemaPathSegment>],
) -> Result<deltalake::kernel::StructType, PolarsError> {
    let fields = schema
        .fields()
        .map(|field| {
            let mut field = field.clone();
            let child_paths = paths
                .iter()
                .filter_map(|path| match path.split_first() {
                    Some((DeltaSchemaPathSegment::Field(name), tail))
                        if delta_names_equal(name, &field.name) =>
                    {
                        Some(tail.to_vec())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            field.data_type = rewrite_delta_type_for_logical_maps(field.data_type, &child_paths)?;
            Ok(field)
        })
        .collect::<Result<Vec<_>, PolarsError>>()?;
    deltalake::kernel::StructType::try_new(fields).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: apply logical map schema: {e}").into())
    })
}

#[cfg(feature = "delta")]
fn rewrite_delta_type_for_logical_maps(
    data_type: deltalake::kernel::DataType,
    paths: &[Vec<DeltaSchemaPathSegment>],
) -> Result<deltalake::kernel::DataType, PolarsError> {
    use deltalake::kernel::{ArrayType, DataType, MapType};

    let exact_map = paths.iter().any(Vec::is_empty);
    match data_type {
        DataType::Struct(fields) => Ok(DataType::Struct(Box::new(
            rewrite_delta_struct_for_logical_maps(*fields, paths)?,
        ))),
        DataType::Variant(fields) => Ok(DataType::Variant(Box::new(
            rewrite_delta_struct_for_logical_maps(*fields, paths)?,
        ))),
        DataType::Array(array) => {
            let element_paths = paths
                .iter()
                .filter_map(|path| {
                    let (segment, tail) = path.split_first()?;
                    match segment {
                        DeltaSchemaPathSegment::ArrayElement => Some(tail.to_vec()),
                        DeltaSchemaPathSegment::MapKey if exact_map => {
                            let mut nested = vec![DeltaSchemaPathSegment::Field("key".into())];
                            nested.extend_from_slice(tail);
                            Some(nested)
                        }
                        DeltaSchemaPathSegment::MapValue if exact_map => {
                            let mut nested = vec![DeltaSchemaPathSegment::Field("value".into())];
                            nested.extend_from_slice(tail);
                            Some(nested)
                        }
                        _ => None,
                    }
                })
                .collect::<Vec<_>>();
            let element = rewrite_delta_type_for_logical_maps(array.element_type, &element_paths)?;
            if exact_map {
                let DataType::Struct(entries) = element else {
                    return Err(PolarsError::InvalidOperation(
                        "write_delta: logical MapType requires an array of key/value structs"
                            .into(),
                    ));
                };
                let key = entries
                    .fields()
                    .find(|field| field.name == "key")
                    .ok_or_else(|| {
                        PolarsError::InvalidOperation(
                            "write_delta: logical MapType is missing its key field".into(),
                        )
                    })?;
                let value = entries
                    .fields()
                    .find(|field| field.name == "value")
                    .ok_or_else(|| {
                        PolarsError::InvalidOperation(
                            "write_delta: logical MapType is missing its value field".into(),
                        )
                    })?;
                Ok(MapType::new(
                    key.data_type.clone(),
                    value.data_type.clone(),
                    value.nullable,
                )
                .into())
            } else {
                Ok(ArrayType::new(element, array.contains_null).into())
            }
        }
        DataType::Map(map) => {
            let key_paths = paths
                .iter()
                .filter_map(|path| match path.split_first() {
                    Some((DeltaSchemaPathSegment::MapKey, tail)) => Some(tail.to_vec()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let value_paths = paths
                .iter()
                .filter_map(|path| match path.split_first() {
                    Some((DeltaSchemaPathSegment::MapValue, tail)) => Some(tail.to_vec()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Ok(MapType::new(
                rewrite_delta_type_for_logical_maps(map.key_type, &key_paths)?,
                rewrite_delta_type_for_logical_maps(map.value_type, &value_paths)?,
                map.value_contains_null,
            )
            .into())
        }
        other if exact_map => Err(PolarsError::InvalidOperation(
            format!("write_delta: cannot encode {other:?} as a logical MapType").into(),
        )),
        other => Ok(other),
    }
}

#[cfg(feature = "delta")]
fn apply_logical_map_paths(
    schema: deltalake::kernel::StructType,
    paths: &[String],
) -> Result<deltalake::kernel::StructType, PolarsError> {
    if paths.is_empty() {
        return Ok(schema);
    }
    let parsed = paths
        .iter()
        .map(|path| parse_delta_schema_path(path))
        .collect::<Result<Vec<_>, _>>()?;
    rewrite_delta_struct_for_logical_maps(schema, &parsed)
}

#[cfg(feature = "delta")]
async fn delta_schema_from_frame(
    df: &polars::prelude::DataFrame,
    logical_map_paths: &[String],
) -> Result<deltalake::kernel::StructType, PolarsError> {
    use deltalake::operations::convert_to_delta::ConvertToDeltaBuilder;
    use std::fs;

    let scratch = std::env::temp_dir().join(format!(
        "sparkless_delta_schema_{:016x}",
        rand::random::<u64>()
    ));
    fs::create_dir_all(&scratch).map_err(|e| {
        PolarsError::ComputeError(
            format!("write_delta: create schema scratch directory: {e}").into(),
        )
    })?;
    let result = async {
        let parquet_path = scratch.join("schema.parquet");
        let mut file = fs::File::create(&parquet_path).map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: create schema parquet: {e}").into())
        })?;
        // Schema inference needs the physical column definitions, not the input rows. Keeping
        // this file empty avoids copying the entire batch into the temporary schema table.
        let mut df_mut = df.slice(0, 0);
        polars::prelude::ParquetWriter::new(&mut file)
            .finish(&mut df_mut)
            .map_err(|e| {
                PolarsError::ComputeError(format!("write_delta: write schema parquet: {e}").into())
            })?;
        drop(file);
        let table_uri = path_to_table_uri(&scratch)?;
        let table = ConvertToDeltaBuilder::new()
            .with_location(table_uri)
            .await
            .map_err(|e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("write_delta: build merged schema: {e}").into())
            })?;
        let snapshot = table.snapshot().map_err(|e: deltalake::DeltaTableError| {
            PolarsError::ComputeError(format!("write_delta: merged schema snapshot: {e}").into())
        })?;
        let schema = snapshot.metadata().parse_schema().map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: parse merged schema: {e}").into())
        })?;
        apply_logical_map_paths(schema, logical_map_paths)
    }
    .await;
    let _ = fs::remove_dir_all(&scratch);
    result
}

#[cfg(feature = "delta")]
fn resolve_delta_null_columns(
    df: &polars::prelude::DataFrame,
    existing_schema: &deltalake::kernel::StructType,
) -> Result<polars::prelude::DataFrame, PolarsError> {
    use polars::prelude::{DataType, IntoLazy, col};

    let input_schema = df.schema();
    let expressions = input_schema
        .iter()
        .filter_map(|(name, dtype)| {
            if dtype != &DataType::Null {
                return None;
            }
            existing_schema
                .fields()
                .find(|field| delta_names_equal(&field.name, name.as_str()))
                .map(|field| {
                    delta_schema_type_to_polars(&field.data_type).map(|target_dtype| {
                        col(name.as_str())
                            .strict_cast(target_dtype)
                            .alias(name.as_str())
                    })
                })
        })
        .collect::<Result<Vec<_>, PolarsError>>()?;

    if expressions.is_empty() {
        return Ok(df.clone());
    }
    df.clone().lazy().with_columns(expressions).collect()
}

#[cfg(feature = "delta")]
fn merge_delta_struct_schemas(
    existing: &deltalake::kernel::StructType,
    incoming: &deltalake::kernel::StructType,
) -> Result<deltalake::kernel::StructType, PolarsError> {
    let mut fields: Vec<_> = existing.fields().cloned().collect();
    for incoming_field in incoming.fields() {
        if let Some(existing_field) = fields
            .iter_mut()
            .find(|field| delta_names_equal(&field.name, &incoming_field.name))
        {
            let old_field = existing_field.clone();
            let mut merged_field = incoming_field.clone();
            // Delta resolves names case-insensitively by default while preserving the name
            // already stored in the table schema.
            merged_field.name = old_field.name.clone();
            merged_field.data_type =
                merge_delta_data_types(&old_field.data_type, &incoming_field.data_type)?;
            merged_field.nullable |= old_field.nullable;
            for (key, value) in old_field.metadata {
                merged_field.metadata.entry(key).or_insert(value);
            }
            *existing_field = merged_field;
        } else {
            fields.push(incoming_field.clone());
        }
    }
    deltalake::kernel::StructType::try_new(fields).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: merge schema fields: {e}").into())
    })
}

#[cfg(feature = "delta")]
fn merge_delta_data_types(
    existing: &deltalake::kernel::DataType,
    incoming: &deltalake::kernel::DataType,
) -> Result<deltalake::kernel::DataType, PolarsError> {
    use deltalake::kernel::{ArrayType, DataType, MapType, PrimitiveType};

    fn integer_rank(data_type: &PrimitiveType) -> Option<u8> {
        match data_type {
            PrimitiveType::Byte => Some(0),
            PrimitiveType::Short => Some(1),
            PrimitiveType::Integer => Some(2),
            PrimitiveType::Long => Some(3),
            _ => None,
        }
    }

    fn primitive_merge(
        existing: &PrimitiveType,
        incoming: &PrimitiveType,
    ) -> Option<PrimitiveType> {
        if existing == incoming {
            return Some(existing.clone());
        }
        if matches!((integer_rank(incoming), integer_rank(existing)), (Some(new), Some(old)) if new <= old)
        {
            return Some(existing.clone());
        }
        match (existing, incoming) {
            (PrimitiveType::Double, PrimitiveType::Float) => Some(PrimitiveType::Double),
            (PrimitiveType::Decimal(old), PrimitiveType::Decimal(new))
                if old.scale() == new.scale() && new.precision() <= old.precision() =>
            {
                PrimitiveType::decimal(old.precision().max(new.precision()), old.scale()).ok()
            }
            _ => None,
        }
    }

    match (existing, incoming) {
        (DataType::Struct(existing), DataType::Struct(incoming)) => Ok(DataType::Struct(Box::new(
            merge_delta_struct_schemas(existing, incoming)?,
        ))),
        (DataType::Variant(existing), DataType::Variant(incoming)) => Ok(DataType::Variant(
            Box::new(merge_delta_struct_schemas(existing, incoming)?),
        )),
        (DataType::Array(existing), DataType::Array(incoming)) => Ok(ArrayType::new(
            merge_delta_data_types(&existing.element_type, &incoming.element_type)?,
            existing.contains_null || incoming.contains_null,
        )
        .into()),
        (DataType::Map(existing), DataType::Map(incoming)) => Ok(MapType::new(
            merge_delta_data_types(&existing.key_type, &incoming.key_type)?,
            merge_delta_data_types(&existing.value_type, &incoming.value_type)?,
            existing.value_contains_null || incoming.value_contains_null,
        )
        .into()),
        (DataType::Primitive(existing), DataType::Primitive(incoming)) => {
            primitive_merge(existing, incoming)
                .map(DataType::Primitive)
                .ok_or_else(|| {
                    PolarsError::InvalidOperation(
                        format!("incompatible Delta schema types: {existing:?} and {incoming:?}")
                            .into(),
                    )
                })
        }
        _ if existing == incoming => Ok(existing.clone()),
        _ => Err(PolarsError::InvalidOperation(
            format!("incompatible Delta schema types: {existing:?} and {incoming:?}").into(),
        )),
    }
}

#[cfg(feature = "delta")]
fn validate_delta_schema_for_write(
    target: &deltalake::kernel::StructType,
    incoming: &deltalake::kernel::StructType,
    allow_extra: bool,
) -> Result<(), PolarsError> {
    use deltalake::kernel::{DataType, PrimitiveType};

    fn integer_rank(data_type: &PrimitiveType) -> Option<u8> {
        match data_type {
            PrimitiveType::Byte => Some(0),
            PrimitiveType::Short => Some(1),
            PrimitiveType::Integer => Some(2),
            PrimitiveType::Long => Some(3),
            _ => None,
        }
    }

    fn can_cast_type(
        incoming: &DataType,
        target: &DataType,
        allow_extra: bool,
    ) -> Result<(), PolarsError> {
        match (incoming, target) {
            (DataType::Primitive(source), DataType::Primitive(target)) => {
                let compatible = source == target
                    || matches!((integer_rank(source), integer_rank(target)), (Some(a), Some(b)) if a <= b)
                    || matches!(
                        (source, target),
                        (PrimitiveType::Float, PrimitiveType::Double)
                    )
                    || matches!((source, target), (PrimitiveType::Decimal(a), PrimitiveType::Decimal(b)) if a.scale() == b.scale() && a.precision() <= b.precision());
                if compatible {
                    Ok(())
                } else {
                    Err(PolarsError::InvalidOperation(
                        format!("incompatible Delta schema types: {source:?} cannot be written as {target:?}").into(),
                    ))
                }
            }
            (DataType::Struct(source), DataType::Struct(target))
            | (DataType::Variant(source), DataType::Variant(target)) => {
                validate_delta_schema_for_write(target, source, allow_extra)
            }
            (DataType::Array(source), DataType::Array(target)) => {
                can_cast_type(&source.element_type, &target.element_type, allow_extra)
            }
            (DataType::Map(source), DataType::Map(target)) => {
                can_cast_type(&source.key_type, &target.key_type, false)?;
                can_cast_type(&source.value_type, &target.value_type, allow_extra)
            }
            _ if incoming == target => Ok(()),
            _ => Err(PolarsError::InvalidOperation(
                format!(
                    "incompatible Delta schema types: {incoming:?} cannot be written as {target:?}"
                )
                .into(),
            )),
        }
    }

    for incoming_field in incoming.fields() {
        let target_field = match target
            .fields()
            .find(|field| delta_names_equal(&field.name, &incoming_field.name))
        {
            Some(field) => field,
            None if allow_extra => continue,
            None => {
                return Err(PolarsError::InvalidOperation(
                    format!("write_delta: column '{}' is not present in the existing Delta schema; enable mergeSchema or overwriteSchema", incoming_field.name).into(),
                ));
            }
        };
        can_cast_type(
            &incoming_field.data_type,
            &target_field.data_type,
            allow_extra,
        )
        .map_err(|error| {
            PolarsError::InvalidOperation(
                format!("write_delta: column '{}': {error}", incoming_field.name).into(),
            )
        })?;
    }

    for target_field in target.fields() {
        let incoming_field = incoming
            .fields()
            .find(|field| delta_names_equal(&field.name, &target_field.name));
        if incoming_field.is_none() && !target_field.nullable {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "write_delta: required column '{}' is missing from the input",
                    target_field.name
                )
                .into(),
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "delta")]
fn validate_non_nullable_values(
    df: &polars::prelude::DataFrame,
    target: &deltalake::kernel::StructType,
) -> Result<(), PolarsError> {
    use deltalake::kernel::DataType as DeltaDataType;
    use polars::prelude::AnyValue;

    fn has_required_descendant(data_type: &DeltaDataType) -> bool {
        match data_type {
            DeltaDataType::Struct(fields) | DeltaDataType::Variant(fields) => fields
                .fields()
                .any(|field| !field.nullable || has_required_descendant(&field.data_type)),
            DeltaDataType::Array(array) => {
                !array.contains_null || has_required_descendant(&array.element_type)
            }
            DeltaDataType::Map(map) => {
                !map.value_contains_null || has_required_descendant(&map.value_type)
            }
            DeltaDataType::Primitive(_) => false,
        }
    }

    fn validate_value(
        value: &AnyValue<'_>,
        data_type: &DeltaDataType,
        nullable: bool,
        path: &str,
    ) -> Result<(), PolarsError> {
        if matches!(value, AnyValue::Null) {
            return if nullable {
                Ok(())
            } else {
                Err(PolarsError::InvalidOperation(
                    format!("write_delta: non-nullable field '{path}' contains NULL values").into(),
                ))
            };
        }

        fn validate_struct_values(
            values: &[AnyValue<'_>],
            physical_fields: &[polars::prelude::Field],
            target_fields: &deltalake::kernel::StructType,
            path: &str,
        ) -> Result<(), PolarsError> {
            for target_field in target_fields.fields() {
                if target_field.nullable && !has_required_descendant(&target_field.data_type) {
                    continue;
                }
                let source = physical_fields
                    .iter()
                    .position(|field| field.name().as_str() == target_field.name)
                    .and_then(|index| values.get(index))
                    .ok_or_else(|| {
                        PolarsError::ColumnNotFound(
                            format!(
                                "write_delta: missing nested field '{}.{}'",
                                path, target_field.name
                            )
                            .into(),
                        )
                    })?;
                validate_value(
                    source,
                    &target_field.data_type,
                    target_field.nullable,
                    &format!("{path}.{}", target_field.name),
                )?;
            }
            Ok(())
        }

        match data_type {
            DeltaDataType::Struct(target_fields) | DeltaDataType::Variant(target_fields) => {
                match value {
                    AnyValue::Struct(_, _, physical_fields) => {
                        let values = value._iter_struct_av().collect::<Vec<_>>();
                        validate_struct_values(&values, physical_fields, target_fields, path)
                    }
                    AnyValue::StructOwned(payload) => {
                        let (values, physical_fields) = &**payload;
                        validate_struct_values(values, physical_fields, target_fields, path)
                    }
                    other => Err(PolarsError::InvalidOperation(
                        format!("write_delta: expected struct at '{path}', found {other:?}").into(),
                    )),
                }
            }
            DeltaDataType::Array(array) => {
                if array.contains_null && !has_required_descendant(&array.element_type) {
                    return Ok(());
                }
                let AnyValue::List(values) = value else {
                    return Err(PolarsError::InvalidOperation(
                        format!("write_delta: expected array at '{path}', found {value:?}").into(),
                    ));
                };
                for index in 0..values.len() {
                    let item = values.get(index)?;
                    validate_value(
                        &item,
                        &array.element_type,
                        array.contains_null,
                        &format!("{path}[{index}]"),
                    )?;
                }
                Ok(())
            }
            DeltaDataType::Map(map) => {
                let AnyValue::List(entries) = value else {
                    return Err(PolarsError::InvalidOperation(
                        format!("write_delta: expected map at '{path}', found {value:?}").into(),
                    ));
                };
                for index in 0..entries.len() {
                    let entry = entries.get(index)?;
                    let (AnyValue::Struct(_, _, _) | AnyValue::StructOwned(_)) = &entry else {
                        return Err(PolarsError::InvalidOperation(
                            format!("write_delta: invalid map entry at '{path}[{index}]'").into(),
                        ));
                    };
                    let (values, physical_fields): (
                        Vec<AnyValue<'_>>,
                        Vec<polars::prelude::Field>,
                    ) = match &entry {
                        AnyValue::Struct(_, _, fields) => {
                            (entry._iter_struct_av().collect(), fields.to_vec())
                        }
                        AnyValue::StructOwned(payload) => (payload.0.clone(), payload.1.clone()),
                        _ => unreachable!(),
                    };
                    for (field_name, field_type, field_nullable) in [
                        ("key", &map.key_type, false),
                        ("value", &map.value_type, map.value_contains_null),
                    ] {
                        let source = physical_fields
                            .iter()
                            .position(|field| field.name().as_str() == field_name)
                            .and_then(|position| values.get(position))
                            .ok_or_else(|| {
                                PolarsError::ColumnNotFound(
                                    format!(
                                        "write_delta: malformed map entry at '{path}[{index}]'"
                                    )
                                    .into(),
                                )
                            })?;
                        validate_value(
                            source,
                            field_type,
                            field_nullable,
                            &format!("{path}[{index}].{field_name}"),
                        )?;
                    }
                }
                Ok(())
            }
            DeltaDataType::Primitive(_) => Ok(()),
        }
    }

    for field in target.fields() {
        if field.nullable && !has_required_descendant(&field.data_type) {
            continue;
        }
        let Some(name) = df
            .get_column_names()
            .into_iter()
            .find(|name| delta_names_equal(name.as_str(), &field.name))
        else {
            continue;
        };
        let column = df.column(name)?;
        for row in 0..df.height() {
            let value = column.get(row)?;
            validate_value(&value, &field.data_type, field.nullable, &field.name)?;
        }
    }
    Ok(())
}

#[cfg(feature = "delta")]
fn align_delta_frame_to_schema(
    df: &polars::prelude::DataFrame,
    incoming_schema: &deltalake::kernel::StructType,
    target_schema: &deltalake::kernel::StructType,
    allow_extra: bool,
) -> Result<polars::prelude::DataFrame, PolarsError> {
    use polars::prelude::{IntoLazy, NULL, col, lit};

    validate_delta_schema_for_write(target_schema, incoming_schema, allow_extra)?;

    let source_polars_schema = df.schema();
    let exprs = target_schema
        .fields()
        .map(|target_field| {
            let incoming_field = incoming_schema
                .fields()
                .find(|field| delta_names_equal(&field.name, &target_field.name));
            let expr = if let Some(incoming_field) = incoming_field {
                let source_dtype =
                    source_polars_schema
                        .get(&incoming_field.name)
                        .ok_or_else(|| {
                            PolarsError::ColumnNotFound(
                                format!(
                                    "write_delta: missing input column '{}'",
                                    incoming_field.name
                                )
                                .into(),
                            )
                        })?;
                align_delta_expr_to_schema_type(
                    col(incoming_field.name.as_str()),
                    source_dtype,
                    &target_field.data_type,
                    true,
                )?
            } else {
                lit(NULL).strict_cast(delta_schema_type_to_polars(&target_field.data_type)?)
            };
            Ok(expr.alias(target_field.name.as_str()))
        })
        .collect::<Result<Vec<_>, PolarsError>>()?;
    let aligned = df.clone().lazy().select(exprs).collect()?;
    validate_non_nullable_values(&aligned, target_schema)?;
    Ok(aligned)
}

#[cfg(feature = "delta")]
fn protocol_for_schema(
    protocol: &deltalake::kernel::Protocol,
    schema: &deltalake::kernel::StructType,
) -> Result<Option<deltalake::kernel::Protocol>, PolarsError> {
    use deltalake::kernel::{DataType, PrimitiveType};

    fn collect_features(
        data_type: &DataType,
        features: &mut std::collections::BTreeSet<&'static str>,
    ) {
        match data_type {
            DataType::Primitive(PrimitiveType::TimestampNtz) => {
                features.insert("timestampNtz");
            }
            DataType::Array(array) => collect_features(&array.element_type, features),
            DataType::Struct(fields) | DataType::Variant(fields) => {
                if matches!(data_type, DataType::Variant(_)) {
                    features.insert("variantType");
                }
                for field in fields.fields() {
                    collect_features(&field.data_type, features);
                }
            }
            DataType::Map(map) => {
                collect_features(&map.key_type, features);
                collect_features(&map.value_type, features);
            }
            DataType::Primitive(_) => {}
        }
    }

    let mut required_features = std::collections::BTreeSet::new();
    for field in schema.fields() {
        collect_features(&field.data_type, &mut required_features);
    }
    if required_features.is_empty() {
        return Ok(None);
    }

    let mut updated = serde_json::to_value(protocol).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: serialize protocol: {e}").into())
    })?;
    let protocol_fields = updated.as_object_mut().ok_or_else(|| {
        PolarsError::ComputeError("write_delta: invalid serialized protocol".into())
    })?;
    for (version_key, minimum) in [("minReaderVersion", 3_i64), ("minWriterVersion", 7_i64)] {
        let current = protocol_fields
            .get(version_key)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(1);
        protocol_fields.insert(
            version_key.to_string(),
            serde_json::Value::from(current.max(minimum)),
        );
    }
    for (feature_key, required) in [
        (
            "readerFeatures",
            required_features.iter().copied().collect::<Vec<_>>(),
        ),
        (
            "writerFeatures",
            required_features.iter().copied().collect::<Vec<_>>(),
        ),
    ] {
        let features = protocol_fields
            .entry(feature_key.to_string())
            .or_insert_with(|| serde_json::Value::Array(Vec::new()))
            .as_array_mut()
            .ok_or_else(|| {
                PolarsError::ComputeError(
                    format!("write_delta: invalid protocol {feature_key}").into(),
                )
            })?;
        for feature in required {
            if !features.iter().any(|value| value.as_str() == Some(feature)) {
                features.push(serde_json::Value::String(feature.to_string()));
            }
        }
    }
    if serde_json::to_value(protocol).ok().as_ref() == Some(&updated) {
        return Ok(None);
    }
    serde_json::from_value(updated).map(Some).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: upgrade protocol: {e}").into())
    })
}

#[cfg(feature = "delta")]
fn parse_delta_timestamp_partition_value(value: &str) -> Result<i64, PolarsError> {
    if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.timestamp_micros());
    }

    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(value, format) {
            // Delta timestamp partition values without an offset are UTC instants. TimestampNTZ
            // uses the same microsecond representation in Polars, with no timezone attached.
            return Ok(timestamp.and_utc().timestamp_micros());
        }
    }

    Err(PolarsError::ComputeError(
        format!("read_delta: invalid timestamp partition value: {value:?}").into(),
    ))
}

#[cfg(feature = "delta")]
fn delta_partition_type_to_polars(
    value: &serde_json::Value,
) -> Result<polars::prelude::DataType, PolarsError> {
    use polars::prelude::{DataType, TimeUnit};

    match value.as_str() {
        Some("string") => Ok(DataType::String),
        Some("boolean") => Ok(DataType::Boolean),
        Some("byte") => Ok(DataType::Int8),
        Some("short") => Ok(DataType::Int16),
        Some("integer") | Some("int") => Ok(DataType::Int32),
        Some("long") => Ok(DataType::Int64),
        Some("float") => Ok(DataType::Float32),
        Some("double") => Ok(DataType::Float64),
        Some("binary") => Ok(DataType::Binary),
        Some("date") => Ok(DataType::Date),
        Some("timestamp") => Ok(DataType::Datetime(
            TimeUnit::Microseconds,
            Some(polars::prelude::TimeZone::UTC),
        )),
        Some("timestamp_ntz") => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
        Some(decimal) if decimal.starts_with("decimal(") && decimal.ends_with(')') => {
            let dimensions = &decimal[8..decimal.len() - 1];
            let (precision, scale) = dimensions.split_once(',').ok_or_else(|| {
                PolarsError::InvalidOperation(
                    format!("unsupported Delta partition type: {value}").into(),
                )
            })?;
            let precision = precision.parse::<usize>().map_err(|e| {
                PolarsError::InvalidOperation(
                    format!("unsupported Delta partition type: {value}: {e}").into(),
                )
            })?;
            let scale = scale.parse::<usize>().map_err(|e| {
                PolarsError::InvalidOperation(
                    format!("unsupported Delta partition type: {value}: {e}").into(),
                )
            })?;
            Ok(DataType::Decimal(precision, scale))
        }
        _ => Err(PolarsError::InvalidOperation(
            format!("unsupported Delta partition type: {value}").into(),
        )),
    }
}

#[cfg(feature = "delta")]
fn delta_schema_type_to_polars(
    data_type: &deltalake::kernel::DataType,
) -> Result<polars::prelude::DataType, PolarsError> {
    use deltalake::kernel::DataType as DeltaDataType;
    use polars::prelude::{DataType, Field};

    match data_type {
        DeltaDataType::Primitive(_) => {
            let serialized = serde_json::to_value(data_type).map_err(|e| {
                PolarsError::ComputeError(format!("read_delta: serialize schema type: {e}").into())
            })?;
            delta_partition_type_to_polars(&serialized)
        }
        DeltaDataType::Struct(fields) | DeltaDataType::Variant(fields) => {
            let fields = fields
                .fields()
                .map(|field| {
                    delta_schema_type_to_polars(&field.data_type)
                        .map(|dtype| Field::new(field.name.clone().into(), dtype))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields))
        }
        DeltaDataType::Array(array) => Ok(DataType::List(Box::new(delta_schema_type_to_polars(
            &array.element_type,
        )?))),
        DeltaDataType::Map(map) => Ok(DataType::List(Box::new(DataType::Struct(vec![
            Field::new("key".into(), delta_schema_type_to_polars(&map.key_type)?),
            Field::new(
                "value".into(),
                delta_schema_type_to_polars(&map.value_type)?,
            ),
        ])))),
    }
}

/// Build an Add action for a parquet file on disk.
#[cfg(feature = "delta")]
fn parquet_file_to_add_action(
    parquet_path: &Path,
    table_root: &Path,
) -> Result<deltalake::kernel::Action, deltalake::DeltaTableError> {
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    let size = std::fs::metadata(parquet_path)
        .map_err(|e| deltalake::DeltaTableError::Io { source: e })?
        .len() as i64;
    let relative_path = parquet_path.strip_prefix(table_root).map_err(|e| {
        deltalake::DeltaTableError::Generic(format!(
            "write_delta: parquet file is outside table root: {e}"
        ))
    })?;
    // Add.path's serializer performs the Delta protocol's percent encoding. Keep the filesystem
    // relative path raw here so escaped partition characters are encoded exactly once.
    let rel_path = relative_path.to_string_lossy().replace('\\', "/");
    let mut partition_values = HashMap::new();
    if let Some(parent) = relative_path.parent() {
        for component in parent.components() {
            let Some(component) = component.as_os_str().to_str() else {
                continue;
            };
            if let Some((name, value)) = component.split_once('=') {
                partition_values.insert(
                    name.to_string(),
                    // Empty string/binary partition values serialize as null in Delta.
                    if value.is_empty() || value == "__HIVE_DEFAULT_PARTITION__" {
                        None
                    } else {
                        Some(
                            percent_encoding::percent_decode_str(value)
                                .decode_utf8()
                                .map_err(|e| {
                                    deltalake::DeltaTableError::Generic(format!(
                                        "write_delta: invalid UTF-8 in partition path value: {e}"
                                    ))
                                })?
                                .into_owned(),
                        )
                    },
                );
            }
        }
    }
    let modification_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| deltalake::DeltaTableError::Generic(e.to_string()))?
        .as_millis() as i64;
    let add = deltalake::kernel::Add {
        path: rel_path,
        partition_values,
        size,
        modification_time,
        data_change: true,
        stats: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    };
    Ok(deltalake::kernel::Action::Add(add))
}

/// Commit actions to an existing Delta table (append or overwrite).
#[cfg(feature = "delta")]
async fn commit_delta_actions(
    table: &mut deltalake::DeltaTable,
    actions: Vec<deltalake::kernel::Action>,
    mode: deltalake::protocol::SaveMode,
    partition_columns: &[String],
) -> Result<(), deltalake::DeltaTableError> {
    use deltalake::kernel::transaction::CommitBuilder;
    use deltalake::protocol::DeltaOperation;

    let table_state = table.snapshot()?;
    let partition_cols = partition_columns.to_vec();
    let operation = DeltaOperation::Write {
        mode,
        partition_by: if partition_cols.is_empty() {
            None
        } else {
            Some(partition_cols)
        },
        predicate: None,
    };
    let finalized = CommitBuilder::default()
        .with_actions(actions)
        .build(Some(table_state), table.log_store().clone(), operation)
        .await?;
    table.state = Some(finalized.snapshot());
    Ok(())
}

/// Write parquet + convert_to_delta for a path that is not yet a Delta table.
#[cfg(feature = "delta")]
async fn write_delta_bootstrap_parquet(
    df: &polars::prelude::DataFrame,
    path: &Path,
    table_uri: &str,
    logical_map_paths: &[String],
) -> Result<(), PolarsError> {
    use deltalake::operations::create::CreateBuilder;

    let existing_files = collect_parquet_paths_under(path)?;
    let schema_files = existing_files.clone();
    let incoming_for_schema = df.clone();
    let schema_frame = tokio::task::spawn_blocking(move || {
        let mut frames = Vec::with_capacity(schema_files.len() + 1);
        for file in schema_files {
            let pl_path = PlRefPath::try_from_path(&file).map_err(|e| {
                PolarsError::ComputeError(
                    format!("write_delta: existing bootstrap Parquet path: {e}").into(),
                )
            })?;
            frames.push(LazyFrame::scan_parquet(
                pl_path,
                ScanArgsParquet::default(),
            )?);
        }
        frames.push(incoming_for_schema.lazy());
        if frames.len() == 1 {
            frames.pop().expect("one incoming schema frame").collect()
        } else {
            concat_lazy_frames_diagonal(frames)?.collect()
        }
    })
    .await
    .map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: merge bootstrap schemas: {e}").into())
    })??;
    let schema = delta_schema_from_frame(&schema_frame, logical_map_paths).await?;

    write_parquet_part(df, path, logical_map_paths)?;
    let actions = collect_parquet_paths_under(path)?
        .iter()
        .map(|file| parquet_file_to_add_action(file, path))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: add bootstrap parquet: {e}").into())
        })?;
    CreateBuilder::new()
        .with_location(table_uri)
        .with_columns(schema.fields().cloned())
        .with_actions(actions)
        .await
        .map_err(|e: deltalake::DeltaTableError| {
            PolarsError::ComputeError(format!("write_delta: create table: {e}").into())
        })?;
    Ok(())
}

/// Write this DataFrame to a Delta table at the given path.
/// If `overwrite` is true, replaces the table; otherwise appends.
/// When `merge_schema` is true and appending, new rows are aligned to the union of existing and
/// incoming columns, and the Delta metadata is updated to match.
/// Existing Delta tables are overwritten transactionally; plain paths are replaced before bootstrapping.
#[cfg(feature = "delta")]
pub fn write_delta(
    df: &polars::prelude::DataFrame,
    path: impl AsRef<Path>,
    overwrite: bool,
    merge_schema: bool,
) -> Result<(), PolarsError> {
    write_delta_with_options(df, path, overwrite, merge_schema, &[])
}

/// Write a Delta table with optional partition columns.
#[cfg(feature = "delta")]
pub fn write_delta_with_options(
    df: &polars::prelude::DataFrame,
    path: impl AsRef<Path>,
    overwrite: bool,
    merge_schema: bool,
    partition_by: &[String],
) -> Result<(), PolarsError> {
    write_delta_with_save_options(
        df,
        path,
        if overwrite {
            crate::dataframe::SaveMode::Overwrite
        } else {
            crate::dataframe::SaveMode::Append
        },
        merge_schema,
        false,
        partition_by,
    )
}

/// Write Delta data with Spark-style save mode and schema evolution options.
#[cfg(feature = "delta")]
pub fn write_delta_with_save_options(
    df: &polars::prelude::DataFrame,
    path: impl AsRef<Path>,
    save_mode: crate::dataframe::SaveMode,
    merge_schema: bool,
    overwrite_schema: bool,
    partition_by: &[String],
) -> Result<(), PolarsError> {
    write_delta_with_save_options_and_logical_map_paths(
        df,
        path,
        save_mode,
        merge_schema,
        overwrite_schema,
        partition_by,
        &[],
    )
}

#[cfg(feature = "delta")]
pub fn write_delta_with_save_options_and_logical_map_paths(
    df: &polars::prelude::DataFrame,
    path: impl AsRef<Path>,
    save_mode: crate::dataframe::SaveMode,
    merge_schema: bool,
    overwrite_schema: bool,
    partition_by: &[String],
    logical_map_paths: &[String],
) -> Result<(), PolarsError> {
    let normalized = normalize_delta_path(path.as_ref())?;
    let path = robin_sparkless_core::maybe_confine_files_path(&normalized).map_err(|e| {
        PolarsError::InvalidOperation(format!("write_delta path confinement: {e}").into())
    })?;
    let path = path.as_path();
    if save_mode == crate::dataframe::SaveMode::ErrorIfExists
        && std::fs::symlink_metadata(path).is_ok()
    {
        return Err(PolarsError::InvalidOperation(
            "Delta table already exists (save mode ErrorIfExists)".into(),
        ));
    }
    let replacing_plain_path = save_mode == crate::dataframe::SaveMode::Overwrite
        && path.exists()
        && !path.join("_delta_log").exists();
    if replacing_plain_path {
        let parent = path.parent().ok_or_else(|| {
            PolarsError::InvalidOperation(
                "write_delta: cannot stage overwrite without a parent directory".into(),
            )
        })?;
        let name = path.file_name().ok_or_else(|| {
            PolarsError::InvalidOperation(
                "write_delta: cannot stage overwrite at filesystem root".into(),
            )
        })?;
        let staging = parent.join(format!(
            ".{}.sparkless_delta_stage_{:016x}",
            name.to_string_lossy(),
            rand::random::<u64>()
        ));
        let staged = write_delta_with_save_options_inner(
            df,
            &staging,
            save_mode,
            merge_schema,
            overwrite_schema,
            partition_by,
            logical_map_paths,
        );
        if let Err(error) = staged {
            let _ = remove_delta_path(&staging);
            return Err(error);
        }
        if let Err(error) = replace_delta_path_with_staged(path, &staging) {
            let _ = remove_delta_path(&staging);
            return Err(error);
        }
        return Ok(());
    }

    write_delta_with_save_options_inner(
        df,
        path,
        save_mode,
        merge_schema,
        overwrite_schema,
        partition_by,
        logical_map_paths,
    )
}

#[cfg(feature = "delta")]
fn remove_delta_path(path: &Path) -> Result<(), PolarsError> {
    use std::fs;

    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(PolarsError::ComputeError(
                format!("write_delta: inspect path {}: {error}", path.display()).into(),
            ));
        }
    };
    if metadata.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
    .map_err(|e| {
        PolarsError::ComputeError(
            format!("write_delta: remove path {}: {e}", path.display()).into(),
        )
    })
}

#[cfg(feature = "delta")]
fn replace_delta_path_with_staged(path: &Path, staging: &Path) -> Result<(), PolarsError> {
    use std::fs;

    let parent = path.parent().ok_or_else(|| {
        PolarsError::InvalidOperation("write_delta: cannot replace filesystem root".into())
    })?;
    let name = path.file_name().ok_or_else(|| {
        PolarsError::InvalidOperation("write_delta: cannot replace filesystem root".into())
    })?;
    let backup = parent.join(format!(
        ".{}.sparkless_delta_backup_{:016x}",
        name.to_string_lossy(),
        rand::random::<u64>()
    ));
    fs::rename(path, &backup).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: stage old path: {e}").into())
    })?;
    if let Err(error) = fs::rename(staging, path) {
        let rollback = fs::rename(&backup, path);
        return Err(PolarsError::ComputeError(
            format!(
                "write_delta: install staged table: {error}{}",
                rollback
                    .err()
                    .map(|rollback| format!("; restoring original path failed: {rollback}"))
                    .unwrap_or_default()
            )
            .into(),
        ));
    }
    remove_delta_path(&backup)
}

#[cfg(feature = "delta")]
fn write_delta_with_save_options_inner(
    df: &polars::prelude::DataFrame,
    path: &Path,
    save_mode: crate::dataframe::SaveMode,
    merge_schema: bool,
    overwrite_schema: bool,
    partition_by: &[String],
    logical_map_paths: &[String],
) -> Result<(), PolarsError> {
    use futures::TryStreamExt;
    use std::fs;
    use tokio::runtime::Runtime;

    if overwrite_schema && save_mode != crate::dataframe::SaveMode::Overwrite {
        return Err(PolarsError::InvalidOperation(
            "write_delta: overwriteSchema requires overwrite mode".into(),
        ));
    }
    let overwrite = save_mode == crate::dataframe::SaveMode::Overwrite;
    let partition_by = partition_by.to_vec();
    let mut partition_names = std::collections::HashSet::new();
    for name in &partition_by {
        if !partition_names.insert(name.to_lowercase()) {
            return Err(PolarsError::InvalidOperation(
                format!("duplicate Delta partition column: {name}").into(),
            ));
        }
        delta_partition_field(df, name)?;
    }
    fs::create_dir_all(path).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create_dir_all: {}", e).into())
    })?;
    reject_symlinks_recursively(&path.join("_delta_log"), "write_delta")?;
    let table_uri = path_to_table_uri(path)?;

    let rt = Runtime::new().map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: failed to create runtime: {}", e).into())
    })?;

    let url = Url::parse(table_uri.as_str()).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: invalid table URI: {}", e).into())
    })?;

    rt.block_on(async {
        let builder = deltalake::DeltaTableBuilder::from_url(url.clone()).map_err(
            |e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("write_delta: {}", e).into())
            },
        )?;

        let load_result = builder.load().await;

        match load_result {
            Ok(mut table) => {
                match save_mode {
                    crate::dataframe::SaveMode::Ignore => return Ok(()),
                    crate::dataframe::SaveMode::ErrorIfExists => {
                        return Err(PolarsError::InvalidOperation(
                            "Delta table already exists (save mode ErrorIfExists)".into(),
                        ));
                    }
                    crate::dataframe::SaveMode::Append
                    | crate::dataframe::SaveMode::Overwrite => {}
                }
                if df.height() == 0 && df.width() == 0 {
                    return Ok(());
                }
                let table_state = table.snapshot().map_err(|e: deltalake::DeltaTableError| {
                    PolarsError::ComputeError(format!("write_delta: snapshot: {}", e).into())
                })?;
                let existing_partition_columns =
                    table_state.metadata().partition_columns().to_vec();
                let replace_table_definition = overwrite && overwrite_schema;
                let requested_partition_names = partition_by
                    .iter()
                    .map(|name| name.to_lowercase())
                    .collect::<std::collections::HashSet<_>>();
                let existing_partition_names = existing_partition_columns
                    .iter()
                    .map(|name| name.to_lowercase())
                    .collect::<std::collections::HashSet<_>>();
                let requested_partitions_match =
                    requested_partition_names == existing_partition_names;
                if !replace_table_definition
                    && !partition_by.is_empty()
                    && !requested_partitions_match
                {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "write_delta: requested partition columns {:?} do not match the existing Delta table partition columns {:?}",
                            partition_by, existing_partition_columns
                        )
                        .into(),
                    ));
                }
                let effective_partition_columns = if replace_table_definition {
                    partition_by.clone()
                } else {
                    existing_partition_columns.clone()
                };
                let existing_schema = table_state.metadata().parse_schema().map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: parse existing schema: {e}").into(),
                    )
                })?;
                if delta_schema_uses_column_mapping(&existing_schema) {
                    return Err(PolarsError::InvalidOperation(
                        "write_delta: writes to Delta tables with column mapping are not supported; refusing to create data files with logical column names".into(),
                    ));
                }
                let input_to_resolve = df.clone();
                let schema_for_null_columns = existing_schema.clone();
                let resolved_df = tokio::task::spawn_blocking(move || {
                    resolve_delta_null_columns(&input_to_resolve, &schema_for_null_columns)
                })
                .await
                .map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: resolve null input columns: {e}").into(),
                    )
                })??;
                let mut incoming_map_paths = logical_map_paths.to_vec();
                let original_schema = df.schema();
                for field in existing_schema.fields() {
                    let null_input = original_schema
                        .get(field.name.as_str())
                        .or_else(|| {
                            original_schema.iter().find_map(|(name, dtype)| {
                                delta_names_equal(name.as_str(), &field.name).then_some(dtype)
                            })
                        })
                        .is_some_and(|dtype| *dtype == polars::prelude::DataType::Null);
                    if !null_input {
                        continue;
                    }
                    let root = serde_json::to_string(&field.name).map_err(|error| {
                        PolarsError::ComputeError(
                            format!("write_delta: encode schema field name: {error}").into(),
                        )
                    })?;
                    incoming_map_paths.extend(
                        logical_map_paths_from_schema(&existing_schema)
                            .into_iter()
                            .filter(|path| path == &root || path.starts_with(&format!("{root}.")) || path.starts_with(&format!("{root}[")) || path.starts_with(&format!("{root}{{"))),
                    );
                }
                incoming_map_paths.sort();
                incoming_map_paths.dedup();
                let incoming_schema =
                    delta_schema_from_frame(&resolved_df, &incoming_map_paths).await?;
                let target_schema = if overwrite_schema {
                    incoming_schema.clone()
                } else if merge_schema {
                    merge_delta_struct_schemas(&existing_schema, &incoming_schema)?
                } else {
                    existing_schema.clone()
                };
                let target_logical_map_paths = logical_map_paths_from_schema(&target_schema);
                let schema_changed = target_schema != existing_schema;
                let schema_to_align = target_schema.clone();
                let input_to_align = resolved_df.clone();
                let incoming_to_align = incoming_schema.clone();
                let aligned_df = tokio::task::spawn_blocking(move || {
                    align_delta_frame_to_schema(
                        &input_to_align,
                        &incoming_to_align,
                        &schema_to_align,
                        false,
                    )
                })
                .await
                .map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: align input to table schema: {e}").into(),
                    )
                })??;
                for name in &effective_partition_columns {
                    let incoming_field = delta_partition_field(&aligned_df, name)?;
                    if replace_table_definition {
                        continue;
                    }
                    let existing_field = existing_schema
                        .fields()
                        .find(|field| delta_names_equal(&field.name, name))
                        .ok_or_else(|| {
                            PolarsError::ComputeError(
                                format!(
                                    "write_delta: partition column '{name}' is missing from the existing schema"
                                )
                                .into(),
                            )
                        })?;
                    if incoming_field.data_type != existing_field.data_type {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "write_delta: incompatible type for partition column '{name}': existing Delta type is {}, incoming type is {}",
                                existing_field.data_type, incoming_field.data_type
                            )
                            .into(),
                        ));
                    }
                }
                let mut actions: Vec<deltalake::kernel::Action> = Vec::new();

                if overwrite {
                    let eager = table_state.snapshot();
                    actions = eager
                        .file_views(&*table.log_store(), None)
                        .map_ok(|v: deltalake::kernel::LogicalFileView| {
                            deltalake::kernel::Action::Remove(v.remove_action(true))
                        })
                        .try_collect()
                        .await
                        .map_err(|e: deltalake::DeltaTableError| {
                            PolarsError::ComputeError(
                                format!("write_delta: file_views: {}", e).into(),
                            )
                        })?;
                }

                let partition_columns_changed =
                    effective_partition_columns != existing_partition_columns;
                if schema_changed || partition_columns_changed {
                    use deltalake::kernel::MetadataExt;

                    let mut updated_metadata = table_state.metadata().clone();
                    if schema_changed {
                        updated_metadata = updated_metadata
                            .with_schema(&target_schema)
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("write_delta: update schema metadata: {e}").into(),
                                )
                            })?;
                    }
                    let mut metadata_value = serde_json::to_value(updated_metadata).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write_delta: serialize updated metadata: {e}").into(),
                        )
                    })?;
                    metadata_value
                        .as_object_mut()
                        .ok_or_else(|| {
                            PolarsError::ComputeError(
                                "write_delta: updated metadata is not an object".into(),
                            )
                        })?
                        .insert(
                            "partitionColumns".to_string(),
                            serde_json::json!(effective_partition_columns),
                        );
                    let updated_metadata: deltalake::kernel::Metadata =
                        serde_json::from_value(metadata_value).map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write_delta: update partition metadata: {e}").into(),
                            )
                        })?;
                    if let Some(protocol) =
                        protocol_for_schema(table_state.protocol(), &target_schema)?
                    {
                        actions.push(deltalake::kernel::Action::Protocol(protocol));
                    }
                    actions.push(deltalake::kernel::Action::Metadata(updated_metadata));
                }

                if effective_partition_columns.is_empty() {
                    let parquet_path =
                        write_parquet_part(&aligned_df, path, &target_logical_map_paths)?;
                    let add = parquet_file_to_add_action(&parquet_path, path).map_err(|e| {
                        PolarsError::ComputeError(format!("write_delta: add action: {}", e).into())
                    })?;
                    actions.push(add);
                } else {
                    actions.extend(write_partitioned_delta_parts(
                        &aligned_df,
                        path,
                        &effective_partition_columns,
                        &target_logical_map_paths,
                    ).await?);
                }
                let mode = if overwrite {
                    deltalake::protocol::SaveMode::Overwrite
                } else {
                    deltalake::protocol::SaveMode::Append
                };
                commit_delta_actions(
                    &mut table,
                    actions,
                    mode,
                    &effective_partition_columns,
                )
                    .await
                    .map_err(|e: deltalake::DeltaTableError| {
                        PolarsError::ComputeError(format!("write_delta: commit: {}", e).into())
                    })?;
                Ok(())
            }
            Err(e) if is_not_a_table(&e) => {
                if df.height() == 0 && df.width() == 0 {
                    return Ok(());
                }
                if partition_by.is_empty() {
                    write_delta_bootstrap_parquet(df, path, &table_uri, logical_map_paths).await
                } else {
                    write_delta_bootstrap_partitioned(
                        df,
                        path,
                        &table_uri,
                        &partition_by,
                        logical_map_paths,
                    )
                    .await
                }
            }
            Err(e) => Err(PolarsError::ComputeError(
                format!("write_delta: {}", e).into(),
            )),
        }
    })
}

#[cfg(all(test, feature = "delta"))]
mod confinement_tests {
    use super::{
        canonicalize_path_for_confinement, confine_parquet_uri_to_table_root, path_to_table_uri,
        uri_to_parquet_path,
    };
    use std::fs;
    use tempfile::TempDir;
    use url::Url;

    #[test]
    fn path_to_table_uri_produces_valid_file_url() {
        let dir = TempDir::new().unwrap();
        let table = dir.path().join("table");
        fs::create_dir_all(&table).unwrap();
        let uri = path_to_table_uri(&table).unwrap();
        let parsed = Url::parse(&uri).unwrap();
        assert_eq!(parsed.scheme(), "file");
        assert!(
            !uri.contains("?/"),
            "extended path prefix leaked into URI: {uri}"
        );
    }

    #[test]
    fn uri_to_parquet_path_parses_file_colon_slash() {
        let got = uri_to_parquet_path("file:/private/tmp/foo.parquet").unwrap();
        assert_eq!(got, std::path::PathBuf::from("/private/tmp/foo.parquet"));
    }

    #[test]
    fn rejects_relative_escape_via_parent_dir() {
        let dir = TempDir::new().unwrap();
        let table = dir.path().join("table");
        let outside = dir.path().join("outside.parquet");
        fs::create_dir_all(&table).unwrap();
        fs::write(&outside, b"x").unwrap();
        let err = confine_parquet_uri_to_table_root("../outside.parquet", &table).unwrap_err();
        assert!(err.to_string().contains("escapes table root"));
    }

    #[test]
    fn rejects_absolute_path_outside_table() {
        let dir = TempDir::new().unwrap();
        let table = dir.path().join("table");
        let outside = dir.path().join("outside.parquet");
        fs::create_dir_all(&table).unwrap();
        fs::write(&outside, b"x").unwrap();
        let uri = format!("file://{}", outside.canonicalize().unwrap().display());
        let err = confine_parquet_uri_to_table_root(&uri, &table).unwrap_err();
        assert!(err.to_string().contains("escapes table root"));
    }

    #[test]
    fn allows_parquet_inside_table() {
        let dir = TempDir::new().unwrap();
        let table = dir.path().join("table");
        let inside = table.join("part-00000.parquet");
        fs::create_dir_all(&table).unwrap();
        fs::write(&inside, b"x").unwrap();
        let got = confine_parquet_uri_to_table_root("part-00000.parquet", &table).unwrap();
        assert!(got.starts_with(table.canonicalize().unwrap()));
    }

    #[test]
    fn canonicalize_resolves_parent_dir() {
        let dir = TempDir::new().unwrap();
        let base = dir.path().join("base");
        fs::create_dir_all(&base).unwrap();
        fs::write(dir.path().join("sibling.txt"), b"x").unwrap();
        let resolved = base.join("..").join("sibling.txt");
        let canonical = canonicalize_path_for_confinement(&resolved, &base).unwrap();
        assert_eq!(
            canonical,
            dir.path().join("sibling.txt").canonicalize().unwrap()
        );
    }

    #[test]
    fn rejects_percent_encoded_parent_dir_escape() {
        let dir = TempDir::new().unwrap();
        let table = dir.path().join("table");
        let outside = dir.path().join("outside.parquet");
        fs::create_dir_all(&table).unwrap();
        fs::write(&outside, b"x").unwrap();
        let err = confine_parquet_uri_to_table_root("%2E%2E/outside.parquet", &table).unwrap_err();
        assert!(err.to_string().contains("escapes table root"));
    }

    #[test]
    fn rejects_embedded_file_uri_under_table_prefix() {
        let dir = TempDir::new().unwrap();
        let table = dir.path().join("table");
        let outside = dir.path().join("outside.parquet");
        fs::create_dir_all(&table).unwrap();
        fs::write(&outside, b"x").unwrap();
        let embedded = format!(
            "{}/file:{}",
            table.canonicalize().unwrap().display(),
            outside.canonicalize().unwrap().display()
        );
        let err = confine_parquet_uri_to_table_root(&embedded, &table).unwrap_err();
        assert!(err.to_string().contains("escapes table root"));
    }

    #[test]
    fn rejects_file_colon_single_slash_absolute_uri() {
        let dir = TempDir::new().unwrap();
        let table = dir.path().join("table");
        let outside = dir.path().join("outside.parquet");
        fs::create_dir_all(&table).unwrap();
        fs::write(&outside, b"x").unwrap();
        let uri = format!("file:{}", outside.canonicalize().unwrap().display());
        let err = confine_parquet_uri_to_table_root(&uri, &table).unwrap_err();
        assert!(err.to_string().contains("escapes table root"));
    }
}
