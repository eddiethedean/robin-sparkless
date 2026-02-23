//! Delta Lake read/write support (optional `delta` feature).
//! Uses deltalake (delta-rs) with Polars for execution.

use crate::dataframe::DataFrame;
use polars::prelude::{LazyFrame, PlRefPath, PolarsError, ScanArgsParquet, UnionArgs};
use std::path::Path;
use url::Url;

/// Concatenate multiple LazyFrames in order. Returns an error if the slice is empty or concat fails.
#[cfg(feature = "delta")]
fn concat_lazy_frames(lfs: Vec<LazyFrame>) -> Result<LazyFrame, PolarsError> {
    if lfs.is_empty() {
        return Err(PolarsError::ComputeError("read_delta: no files".into()));
    }
    polars::prelude::concat(lfs, UnionArgs::default())
}

/// Read a Delta table at the given path (latest version).
/// Path can be a local path (e.g. `/tmp/table`) or a file URL (`file:///tmp/table`).
#[cfg(feature = "delta")]
pub fn read_delta(path: impl AsRef<Path>, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    read_delta_with_version(path, None, case_sensitive)
}

/// Read a Delta table at the given path, optionally at a specific version (time travel).
#[cfg(feature = "delta")]
pub fn read_delta_with_version(
    path: impl AsRef<Path>,
    version: Option<i64>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use deltalake::DeltaTableBuilder;
    use tokio::runtime::Runtime;

    let path = path.as_ref();
    let table_uri = path_to_table_uri(path)?;

    let rt = Runtime::new().map_err(|e| {
        PolarsError::ComputeError(format!("read_delta: failed to create runtime: {}", e).into())
    })?;

    let url = Url::parse(table_uri.as_str()).map_err(|e| {
        PolarsError::ComputeError(format!("read_delta: invalid table URI: {}", e).into())
    })?;
    let table = rt.block_on(async {
        let builder =
            DeltaTableBuilder::from_url(url.clone()).map_err(|e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("read_delta: {}", e).into())
            })?;
        let result = if let Some(v) = version {
            builder.with_version(v).load().await
        } else {
            builder.load().await
        };
        result.map_err(|e: deltalake::DeltaTableError| {
            PolarsError::ComputeError(format!("read_delta: {}", e).into())
        })
    })?;

    let uris: Vec<String> = table
        .get_file_uris()
        .map_err(|e: deltalake::DeltaTableError| {
            PolarsError::ComputeError(format!("read_delta: get_file_uris: {}", e).into())
        })?
        .collect();
    if uris.is_empty() {
        return Ok(DataFrame::from_polars_with_options(
            polars::prelude::DataFrame::default(),
            case_sensitive,
        ));
    }

    let mut lfs: Vec<LazyFrame> = Vec::with_capacity(uris.len());
    for uri in &uris {
        let parquet_path = uri_to_parquet_path(uri)?;
        let pl_path = PlRefPath::try_from_path(&parquet_path).map_err(|e| {
            PolarsError::ComputeError(format!("read_delta scan_parquet path: {e}").into())
        })?;
        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
        lfs.push(lf);
    }

    let combined = concat_lazy_frames(lfs)?;

    let pl_df = combined.collect()?;
    Ok(DataFrame::from_polars_with_options(pl_df, case_sensitive))
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
    let path_str = abs_path.to_string_lossy();
    #[cfg(target_os = "windows")]
    let uri = format!("file:///{}", path_str.replace('\\', "/"));
    #[cfg(not(target_os = "windows"))]
    let uri = format!("file://{}", path_str);
    Ok(uri)
}

#[cfg(feature = "delta")]
fn uri_to_parquet_path(uri: &str) -> Result<std::path::PathBuf, PolarsError> {
    if let Some(stripped) = uri.strip_prefix("file://") {
        #[cfg(target_os = "windows")]
        {
            let s = stripped.trim_start_matches('/').replace('/', "\\");
            return Ok(std::path::PathBuf::from(s));
        }
        #[cfg(not(target_os = "windows"))]
        {
            let s = stripped.trim_start_matches('/');
            return Ok(std::path::PathBuf::from(format!("/{}", s)));
        }
    }
    // deltalake may return bare absolute paths (e.g. /private/var/.../file.parquet).
    if uri.starts_with('/')
        || (cfg!(target_os = "windows") && uri.len() >= 2 && uri.chars().nth(1) == Some(':'))
    {
        return Ok(std::path::PathBuf::from(uri));
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

/// Align two DataFrames to a merged column order, adding null columns for missing columns.
/// merged_columns: existing names first, then new names not in existing (PySpark mergeSchema order).
#[cfg(feature = "delta")]
fn align_to_merged_schema(
    existing: &polars::prelude::DataFrame,
    new_df: &polars::prelude::DataFrame,
) -> Result<(polars::prelude::DataFrame, polars::prelude::DataFrame), PolarsError> {
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
    let existing_set: std::collections::HashSet<&str> =
        existing_names.iter().map(String::as_str).collect();
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
    let aligned_existing = polars::prelude::DataFrame::new_infer_height(cols_existing)?;
    let aligned_new = polars::prelude::DataFrame::new_infer_height(cols_new)?;
    Ok((aligned_existing, aligned_new))
}

/// Write this DataFrame to a Delta table at the given path.
/// If `overwrite` is true, replaces the table; otherwise appends.
/// When `merge_schema` is true and appending, the result schema is the union of existing and new columns (missing columns filled with null). Fixes #851.
/// Uses Parquet write + convert_to_delta for new/overwrite; appends by reading existing table, concatenating, then overwriting.
#[cfg(feature = "delta")]
pub fn write_delta(
    df: &polars::prelude::DataFrame,
    path: impl AsRef<Path>,
    overwrite: bool,
    merge_schema: bool,
) -> Result<(), PolarsError> {
    use deltalake::operations::convert_to_delta::ConvertToDeltaBuilder;
    use std::fs;
    use tokio::runtime::Runtime;

    let path = path.as_ref();

    if df.height() == 0 {
        return Ok(());
    }

    // Create target directory before resolving URI so deltalake sees an existing path.
    if overwrite {
        if path.exists() {
            let _ = fs::remove_dir_all(path);
        }
        fs::create_dir_all(path).map_err(|e| {
            PolarsError::ComputeError(format!("write_delta: create_dir_all: {}", e).into())
        })?;
    }
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
        let table_result = builder
            .load()
            .await
            .map_err(|e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("write_delta: {}", e).into())
            });

        if overwrite {
            let parquet_path = path.join("part-00000.parquet");
            let mut file =
                std::io::BufWriter::new(fs::File::create(&parquet_path).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write_delta: create parquet file: {}", e).into(),
                    )
                })?);
            let mut df_mut = df.clone();
            polars::prelude::ParquetWriter::new(&mut file)
                .finish(&mut df_mut)
                .map_err(|e| {
                    PolarsError::ComputeError(format!("write_delta: parquet write: {}", e).into())
                })?;
            drop(file);
            ConvertToDeltaBuilder::new()
                .with_location(&table_uri)
                .await
                .map_err(|e: deltalake::DeltaTableError| {
                    PolarsError::ComputeError(
                        format!("write_delta: convert_to_delta: {}", e).into(),
                    )
                })?;
        } else {
            match table_result {
                Ok(table) => {
                    let uris: Vec<String> = table
                        .get_file_uris()
                        .map_err(|e: deltalake::DeltaTableError| {
                            PolarsError::ComputeError(
                                format!("write_delta: get_file_uris: {}", e).into(),
                            )
                        })?
                        .collect();
                    let mut lfs: Vec<LazyFrame> = Vec::with_capacity(uris.len() + 1);
                    for uri in &uris {
                        let parquet_path = uri_to_parquet_path(uri)?;
                        let pl_path = PlRefPath::try_from_path(&parquet_path).map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write_delta scan_parquet path: {e}").into(),
                            )
                        })?;
                        lfs.push(LazyFrame::scan_parquet(
                            pl_path,
                            ScanArgsParquet::default(),
                        )?);
                    }
                    let mut combined = if lfs.is_empty() {
                        polars::prelude::DataFrame::default()
                    } else {
                        concat_lazy_frames(lfs)?.collect()?
                    };
                    let to_append = if merge_schema && combined.width() > 0 {
                        let (aligned_existing, aligned_new) =
                            align_to_merged_schema(&combined, df)?;
                        let mut out = aligned_existing;
                        out.vstack_mut(&aligned_new)?;
                        out
                    } else {
                        combined.vstack_mut(df)?;
                        combined
                    };
                    let mut combined = to_append;
                    let _ = fs::remove_dir_all(path);
                    fs::create_dir_all(path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write_delta: create_dir_all: {}", e).into(),
                        )
                    })?;
                    let parquet_path = path.join("part-00000.parquet");
                    let mut file =
                        std::io::BufWriter::new(fs::File::create(&parquet_path).map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write_delta: create parquet file: {}", e).into(),
                            )
                        })?);
                    polars::prelude::ParquetWriter::new(&mut file)
                        .finish(&mut combined)
                        .map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write_delta: parquet write: {}", e).into(),
                            )
                        })?;
                    drop(file);
                    ConvertToDeltaBuilder::new()
                        .with_location(&table_uri)
                        .await
                        .map_err(|e: deltalake::DeltaTableError| {
                            PolarsError::ComputeError(
                                format!("write_delta: convert_to_delta: {}", e).into(),
                            )
                        })?;
                }
                Err(_) => {
                    fs::create_dir_all(path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write_delta: create_dir_all: {}", e).into(),
                        )
                    })?;
                    let parquet_path = path.join("part-00000.parquet");
                    let mut file =
                        std::io::BufWriter::new(fs::File::create(&parquet_path).map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write_delta: create parquet file: {}", e).into(),
                            )
                        })?);
                    let mut df_mut = df.clone();
                    polars::prelude::ParquetWriter::new(&mut file)
                        .finish(&mut df_mut)
                        .map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write_delta: parquet write: {}", e).into(),
                            )
                        })?;
                    drop(file);
                    ConvertToDeltaBuilder::new()
                        .with_location(&table_uri)
                        .await
                        .map_err(|e: deltalake::DeltaTableError| {
                            PolarsError::ComputeError(
                                format!("write_delta: convert_to_delta: {}", e).into(),
                            )
                        })?;
                }
            }
        }
        Ok(())
    })
}
