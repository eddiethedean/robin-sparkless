//! Delta Lake read/write support (optional `delta` feature).
//! Uses deltalake (delta-rs) with Polars for execution.

use crate::dataframe::DataFrame;
use polars::prelude::{LazyFrame, PlRefPath, PolarsError, ScanArgsParquet, UnionArgs};
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
        if p.is_dir() {
            out.extend(collect_parquet_paths_under_depth(&p, depth + 1)?);
        } else if p.extension().is_some_and(|e| e == "parquet") {
            out.push(p);
        }
    }
    out.sort();
    Ok(out)
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
        let uris: Vec<String> = table
            .get_file_uris()
            .map_err(|e: deltalake::DeltaTableError| {
                PolarsError::ComputeError(format!("read_delta: get_file_uris: {}", e).into())
            })?
            .collect();
        Ok::<_, PolarsError>(uris)
    })();

    match table_result {
        Ok(uris) => {
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

/// Load existing Delta table rows from file URIs (sync; for use inside async write_delta).
#[cfg(feature = "delta")]
fn load_pl_from_parquet_uris(uris: &[String]) -> Result<polars::prelude::DataFrame, PolarsError> {
    use polars::prelude::DataFrame as PlDataFrame;
    if uris.is_empty() {
        return Ok(PlDataFrame::default());
    }
    let mut lfs: Vec<LazyFrame> = Vec::with_capacity(uris.len());
    for uri in uris {
        let parquet_path = uri_to_parquet_path(uri)?;
        let pl_path = PlRefPath::try_from_path(&parquet_path).map_err(|e| {
            PolarsError::ComputeError(format!("read_delta scan_parquet path: {e}").into())
        })?;
        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
        lfs.push(lf);
    }
    let combined = if lfs.len() == 1 {
        lfs.into_iter().next().unwrap()
    } else {
        concat_lazy_frames_diagonal(lfs)?
    };
    combined.collect()
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

/// Build an Add action for a parquet file on disk.
#[cfg(feature = "delta")]
fn parquet_file_to_add_action(
    parquet_path: &Path,
    table_uri: &str,
) -> Result<deltalake::kernel::Action, deltalake::DeltaTableError> {
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    let size = std::fs::metadata(parquet_path)
        .map_err(|e| deltalake::DeltaTableError::Io { source: e })?
        .len() as i64;
    let rel_path = parquet_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("part.parquet")
        .to_string();
    let modification_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| deltalake::DeltaTableError::Generic(e.to_string()))?
        .as_millis() as i64;
    let _ = table_uri; // path is relative to table root
    let add = deltalake::kernel::Add {
        path: rel_path,
        partition_values: HashMap::new(),
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
) -> Result<(), deltalake::DeltaTableError> {
    use deltalake::kernel::transaction::CommitBuilder;
    use deltalake::protocol::DeltaOperation;

    let table_state = table.snapshot()?;
    let partition_cols: Vec<String> = table_state.metadata().partition_columns().to_vec();
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
/// When `merge_schema` is true and appending, schema evolution is applied via delta-rs.
/// Never wipes the table directory with `remove_dir_all` (transactional commits instead).
#[cfg(feature = "delta")]
pub fn write_delta(
    df: &polars::prelude::DataFrame,
    path: impl AsRef<Path>,
    overwrite: bool,
    merge_schema: bool,
) -> Result<(), PolarsError> {
    use futures::TryStreamExt;
    use std::fs;
    use tokio::runtime::Runtime;

    let path = path.as_ref();
    fs::create_dir_all(path).map_err(|e| {
        PolarsError::ComputeError(format!("write_delta: create_dir_all: {}", e).into())
    })?;
    let table_uri = path_to_table_uri(path)?;
    if df.height() == 0 && df.width() == 0 {
        return Ok(());
    }

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
                let table_state = table.snapshot().map_err(|e: deltalake::DeltaTableError| {
                    PolarsError::ComputeError(format!("write_delta: snapshot: {}", e).into())
                })?;
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

                let df_for_part = if merge_schema && !overwrite {
                    let uris: Vec<String> = table
                        .get_file_uris()
                        .map_err(|e: deltalake::DeltaTableError| {
                            PolarsError::ComputeError(
                                format!("write_delta: get_file_uris for merge_schema: {e}").into(),
                            )
                        })?
                        .collect();
                    // Polars collect() may start a runtime; must not run inside block_on.
                    let df_clone = df.clone();
                    let existing_pl =
                        tokio::task::spawn_blocking(move || load_pl_from_parquet_uris(&uris))
                            .await
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("write_delta: load existing for merge_schema: {e}")
                                        .into(),
                                )
                            })??;
                    let (_, aligned_new) =
                        crate::dataframe::align_to_merged_schema_inline(&existing_pl, &df_clone)?;
                    aligned_new
                } else {
                    df.clone()
                };
                let parquet_path = write_parquet_part(&df_for_part, path)?;
                let add = parquet_file_to_add_action(&parquet_path, &table_uri).map_err(|e| {
                    PolarsError::ComputeError(format!("write_delta: add action: {}", e).into())
                })?;
                actions.push(add);
                let mode = if overwrite {
                    deltalake::protocol::SaveMode::Overwrite
                } else {
                    deltalake::protocol::SaveMode::Append
                };
                commit_delta_actions(&mut table, actions, mode)
                    .await
                    .map_err(|e: deltalake::DeltaTableError| {
                        PolarsError::ComputeError(format!("write_delta: commit: {}", e).into())
                    })?;
                Ok(())
            }
            Err(e) if is_not_a_table(&e) => {
                write_delta_bootstrap_parquet(df, path, &table_uri).await
            }
            Err(e) => Err(PolarsError::ComputeError(
                format!("write_delta: {}", e).into(),
            )),
        }
    })
}
