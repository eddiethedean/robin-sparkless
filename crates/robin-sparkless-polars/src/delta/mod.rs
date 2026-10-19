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
fn align_delta_expr_to_dtype(
    expr: polars::prelude::Expr,
    source_dtype: &polars::prelude::DataType,
    target_dtype: &polars::prelude::DataType,
) -> Result<polars::prelude::Expr, PolarsError> {
    use polars::prelude::{DataType, NULL, as_struct, col, lit, when};

    match (source_dtype, target_dtype) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let fields = target_fields
                .iter()
                .map(|target_field| {
                    let name = target_field.name();
                    let target_field_dtype = target_field.dtype();
                    let field_expr = if let Some(source_field) =
                        source_fields.iter().find(|field| field.name() == name)
                    {
                        align_delta_expr_to_dtype(
                            expr.clone().struct_().field_by_name(name),
                            source_field.dtype(),
                            target_field_dtype,
                        )?
                    } else {
                        lit(NULL).strict_cast(target_field_dtype.clone())
                    };
                    Ok(field_expr.alias(name.as_str()))
                })
                .collect::<Result<Vec<_>, PolarsError>>()?;
            let aligned = as_struct(fields);
            Ok(when(expr.clone().is_null())
                .then(lit(NULL).strict_cast(target_dtype.clone()))
                .otherwise(aligned))
        }
        (DataType::List(source_inner), DataType::List(target_inner)) => {
            let item_expr = align_delta_expr_to_dtype(col(""), source_inner, target_inner)?;
            Ok(expr.list().eval(item_expr))
        }
        _ => Ok(expr.strict_cast(target_dtype.clone())),
    }
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

    let path = path.as_ref();

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
            let mut lfs: Vec<LazyFrame> = Vec::with_capacity(files.len());
            for (uri, partition_values) in &files {
                let parquet_path = confine_parquet_uri_to_table_root(uri, &table_root_canonical)?;
                let pl_path = PlRefPath::try_from_path(&parquet_path).map_err(|e| {
                    PolarsError::ComputeError(format!("read_delta scan_parquet path: {e}").into())
                })?;
                let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
                let source_schema = lf.clone().collect_schema()?;
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
                    let target_dtype = delta_schema_type_to_polars(&field.data_type)?;
                    let expr = if let Some(partition_expr) = partition_exprs.remove(&field.name) {
                        partition_expr
                    } else if let Some(source_field) = source_schema.get(&field.name) {
                        align_delta_expr_to_dtype(
                            polars::prelude::col(field.name.as_str()),
                            source_field,
                            &target_dtype,
                        )?
                    } else {
                        polars::prelude::lit(polars::prelude::NULL).cast(target_dtype)
                    };
                    aligned_exprs.push(expr.alias(field.name.as_str()));
                }
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
            let pl_df = combined.collect()?;
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
) -> Result<std::path::PathBuf, PolarsError> {
    use std::fs;
    let part_id: u64 = rand::random();
    let parquet_path = path.join(format!("part-{part_id:016x}.parquet"));
    let mut file = std::io::BufWriter::new(fs::File::create(&parquet_path).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create parquet file: {}", e).into())
    })?);
    let mut df_mut = df.clone();
    polars::prelude::ParquetWriter::new(&mut file)
        .finish(&mut df_mut)
        .map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: parquet write: {}", e).into())
        })?;
    drop(file);
    Ok(parquet_path)
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
fn write_partitioned_parquet(
    df: &polars::prelude::DataFrame,
    path: &Path,
    partition_columns: &[String],
    mode: crate::dataframe::WriteMode,
) -> Result<(), PolarsError> {
    let frame = DataFrame::from_polars(df.clone());
    frame
        .write()
        .mode(mode)
        .format(crate::dataframe::WriteFormat::Parquet)
        .partition_by(partition_columns.iter().cloned())
        .save(path)
}

#[cfg(feature = "delta")]
async fn write_delta_bootstrap_partitioned(
    df: &polars::prelude::DataFrame,
    path: &Path,
    table_uri: &str,
    partition_columns: &[String],
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
    let schema = delta_schema_from_frame(&schema_frame).await?;

    // Stage new files first, then move them into the table only after checking every destination
    // partition directory for symlinks and table-root confinement.
    write_partitioned_delta_parts(df, path, partition_columns)?;

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
fn write_partitioned_delta_parts(
    df: &polars::prelude::DataFrame,
    table_root: &Path,
    partition_columns: &[String],
) -> Result<Vec<deltalake::kernel::Action>, PolarsError> {
    use std::fs;

    fs::create_dir_all(table_root).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create table root: {e}").into())
    })?;
    let staging = table_root.join(format!(
        ".sparkless_delta_write_{:016x}",
        rand::random::<u64>()
    ));
    let result = (|| {
        fs::create_dir(&staging).map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: create staging directory: {e}").into())
        })?;
        write_partitioned_parquet(
            df,
            &staging,
            partition_columns,
            crate::dataframe::WriteMode::Append,
        )?;
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
    })();
    let _ = fs::remove_dir_all(&staging);
    result
}

#[cfg(feature = "delta")]
async fn delta_schema_from_frame(
    df: &polars::prelude::DataFrame,
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
        snapshot.metadata().parse_schema().map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: parse merged schema: {e}").into())
        })
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
                .find(|field| field.name == name.as_str())
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
            .find(|field| field.name == incoming_field.name)
        {
            let old_field = existing_field.clone();
            let mut merged_field = incoming_field.clone();
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
            .find(|field| field.name == incoming_field.name)
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
        if incoming
            .fields()
            .all(|field| field.name != target_field.name)
            && !target_field.nullable
        {
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
            let target_dtype = delta_schema_type_to_polars(&target_field.data_type)?;
            let expr = if incoming_schema
                .fields()
                .any(|field| field.name == target_field.name)
            {
                let source_dtype =
                    source_polars_schema
                        .get(&target_field.name)
                        .ok_or_else(|| {
                            PolarsError::ColumnNotFound(
                                format!(
                                    "write_delta: missing input column '{}'",
                                    target_field.name
                                )
                                .into(),
                            )
                        })?;
                align_delta_expr_to_dtype(
                    col(target_field.name.as_str()),
                    source_dtype,
                    &target_dtype,
                )?
            } else {
                lit(NULL).strict_cast(target_dtype)
            };
            Ok(expr.alias(target_field.name.as_str()))
        })
        .collect::<Result<Vec<_>, PolarsError>>()?;
    df.clone().lazy().select(exprs).collect()
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
) -> Result<(), PolarsError> {
    use deltalake::operations::convert_to_delta::ConvertToDeltaBuilder;
    use std::fs;

    fs::create_dir_all(path).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create_dir_all: {}", e).into())
    })?;
    let part_id: u64 = rand::random();
    let parquet_path = path.join(format!("part-{part_id:016x}.parquet"));
    let mut file = std::io::BufWriter::new(fs::File::create(&parquet_path).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create parquet file: {}", e).into())
    })?);
    let mut df_mut = df.clone();
    polars::prelude::ParquetWriter::new(&mut file)
        .finish(&mut df_mut)
        .map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: parquet write: {}", e).into())
        })?;
    drop(file);
    ConvertToDeltaBuilder::new()
        .with_location(table_uri)
        .await
        .map_err(|e: deltalake::DeltaTableError| {
            PolarsError::ComputeError(format!("write_delta: convert_to_delta: {}", e).into())
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
    use futures::TryStreamExt;
    use std::fs;
    use tokio::runtime::Runtime;

    if overwrite_schema && save_mode != crate::dataframe::SaveMode::Overwrite {
        return Err(PolarsError::InvalidOperation(
            "write_delta: overwriteSchema requires overwrite mode".into(),
        ));
    }
    let overwrite = save_mode == crate::dataframe::SaveMode::Overwrite;
    let path = robin_sparkless_core::maybe_confine_files_path(path.as_ref()).map_err(|e| {
        PolarsError::InvalidOperation(format!("write_delta path confinement: {e}").into())
    })?;
    let path = path.as_path();
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
    // Overwrite should replace a plain Parquet location instead of converting its old files
    // into the initial Delta snapshot. Existing Delta tables are overwritten transactionally
    // below by adding remove actions to the log.
    if overwrite && path.exists() && !path.join("_delta_log").exists() {
        if path.is_dir() {
            fs::remove_dir_all(path).map_err(|e| {
                PolarsError::ComputeError(format!("write_delta: remove existing path: {e}").into())
            })?;
        } else {
            fs::remove_file(path).map_err(|e| {
                PolarsError::ComputeError(format!("write_delta: remove existing path: {e}").into())
            })?;
        }
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
                if !replace_table_definition
                    && !partition_by.is_empty()
                    && partition_by != existing_partition_columns
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
                } else if partition_by.is_empty() {
                    existing_partition_columns.clone()
                } else {
                    partition_by.clone()
                };
                let existing_schema = table_state.metadata().parse_schema().map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: parse existing schema: {e}").into(),
                    )
                })?;
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
                let incoming_schema = delta_schema_from_frame(&resolved_df).await?;
                let target_schema = if overwrite_schema {
                    incoming_schema.clone()
                } else if merge_schema {
                    merge_delta_struct_schemas(&existing_schema, &incoming_schema)?
                } else {
                    existing_schema.clone()
                };
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
                    let incoming_field = delta_partition_field(&resolved_df, name)?;
                    if replace_table_definition {
                        continue;
                    }
                    let existing_field = existing_schema
                        .fields()
                        .find(|field| field.name == *name)
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
                    let parquet_path = write_parquet_part(&aligned_df, path)?;
                    let add = parquet_file_to_add_action(&parquet_path, path).map_err(|e| {
                        PolarsError::ComputeError(format!("write_delta: add action: {}", e).into())
                    })?;
                    actions.push(add);
                } else {
                    actions.extend(write_partitioned_delta_parts(
                        &aligned_df,
                        path,
                        &effective_partition_columns,
                    )?);
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
                    write_delta_bootstrap_parquet(df, path, &table_uri).await
                } else {
                    write_delta_bootstrap_partitioned(df, path, &table_uri, &partition_by).await
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
