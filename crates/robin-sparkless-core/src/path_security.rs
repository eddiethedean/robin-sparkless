//! Path confinement helpers for warehouse-backed tables and local file access.

use std::path::{Component, Path, PathBuf};

use crate::EngineError;

/// Reject table/path names that could escape a base directory via `..` or absolute paths.
pub fn validate_table_name(name: &str) -> Result<(), EngineError> {
    if name.is_empty() {
        return Err(EngineError::User(
            "table name must not be empty".to_string(),
        ));
    }
    if name.contains('\0') {
        return Err(EngineError::User(
            "table name contains NUL byte".to_string(),
        ));
    }
    for component in Path::new(name).components() {
        match component {
            Component::ParentDir => {
                return Err(EngineError::User(format!(
                    "table name '{name}' must not contain '..'"
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(EngineError::User(format!(
                    "table name '{name}' must be relative (no absolute path)"
                )));
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }
    Ok(())
}

/// Join `base` and `name` then ensure the result stays under `base` after canonicalization.
pub fn resolve_path_under_base(base: impl AsRef<Path>, name: &str) -> Result<PathBuf, EngineError> {
    validate_table_name(name)?;
    let base = base.as_ref();
    let joined = base.join(name);
    let canonical_base = base.canonicalize().map_err(|e| {
        EngineError::Io(format!(
            "failed to canonicalize base directory '{}': {e}",
            base.display()
        ))
    })?;
    let canonical = if joined.exists() {
        joined.canonicalize().map_err(|e| {
            EngineError::Io(format!(
                "failed to canonicalize path '{}': {e}",
                joined.display()
            ))
        })?
    } else {
        let parent = joined.parent().unwrap_or(base);
        let file_name = joined
            .file_name()
            .ok_or_else(|| EngineError::User("invalid table path".to_string()))?;
        let canonical_parent = if parent.exists() {
            parent.canonicalize().map_err(|e| {
                EngineError::Io(format!(
                    "failed to canonicalize parent '{}': {e}",
                    parent.display()
                ))
            })?
        } else {
            canonical_base.clone()
        };
        canonical_parent.join(file_name)
    };
    if !canonical.starts_with(&canonical_base) {
        return Err(EngineError::User(format!(
            "path '{}' escapes base directory '{}'",
            canonical.display(),
            canonical_base.display()
        )));
    }
    Ok(canonical)
}

/// When `SPARKLESS_FILES_BASE` is set, confine `path` under that directory.
/// When unset, returns the path unchanged.
pub fn maybe_confine_files_path(path: impl AsRef<Path>) -> Result<PathBuf, EngineError> {
    let path = path.as_ref();
    match std::env::var("SPARKLESS_FILES_BASE") {
        Ok(base) if !base.trim().is_empty() => {
            confine_user_path_under_base(Path::new(base.trim()), path)
        }
        _ => Ok(path.to_path_buf()),
    }
}

/// Resolve a user-supplied filesystem path under `base` (absolute or relative).
pub fn confine_user_path_under_base(
    base: impl AsRef<Path>,
    user_path: impl AsRef<Path>,
) -> Result<PathBuf, EngineError> {
    let base = base.as_ref();
    let user_path = user_path.as_ref();
    let canonical_base = base.canonicalize().map_err(|e| {
        EngineError::Io(format!(
            "failed to canonicalize files base '{}': {e}",
            base.display()
        ))
    })?;
    let joined = if user_path.is_absolute() {
        user_path.to_path_buf()
    } else {
        canonical_base.join(user_path)
    };
    let canonical = if joined.exists() {
        joined.canonicalize().map_err(|e| {
            EngineError::Io(format!(
                "failed to canonicalize path '{}': {e}",
                joined.display()
            ))
        })?
    } else if user_path.is_absolute() {
        let parent = joined.parent().unwrap_or(&canonical_base);
        let file_name = joined
            .file_name()
            .ok_or_else(|| EngineError::User("invalid path".to_string()))?;
        if parent.exists() {
            let canonical_parent = parent.canonicalize().map_err(|e| {
                EngineError::Io(format!(
                    "failed to canonicalize parent '{}': {e}",
                    parent.display()
                ))
            })?;
            if !canonical_parent.starts_with(&canonical_base) {
                return Err(EngineError::User(format!(
                    "path '{}' escapes files base directory '{}'",
                    joined.display(),
                    canonical_base.display()
                )));
            }
            canonical_parent.join(file_name)
        } else {
            return Err(EngineError::User(format!(
                "path '{}' escapes files base directory '{}'",
                joined.display(),
                canonical_base.display()
            )));
        }
    } else {
        let parent = joined.parent().unwrap_or(&canonical_base);
        let file_name = joined
            .file_name()
            .ok_or_else(|| EngineError::User("invalid path".to_string()))?;
        let canonical_parent = if parent.exists() {
            parent.canonicalize().map_err(|e| {
                EngineError::Io(format!(
                    "failed to canonicalize parent '{}': {e}",
                    parent.display()
                ))
            })?
        } else {
            canonical_base.clone()
        };
        canonical_parent.join(file_name)
    };
    if !canonical.starts_with(&canonical_base) {
        return Err(EngineError::User(format!(
            "path '{}' escapes files base directory '{}'",
            canonical.display(),
            canonical_base.display()
        )));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn rejects_parent_dir_in_name() {
        assert!(validate_table_name("../escape").is_err());
    }

    #[test]
    fn resolve_stays_under_base() {
        let dir = TempDir::new().unwrap();
        let base = dir.path().join("warehouse");
        fs::create_dir_all(&base).unwrap();
        let p = resolve_path_under_base(&base, "my_table").unwrap();
        assert!(p.starts_with(base.canonicalize().unwrap()));
    }

    #[test]
    fn resolve_rejects_escape() {
        let dir = TempDir::new().unwrap();
        let base = dir.path().join("warehouse");
        fs::create_dir_all(&base).unwrap();
        assert!(resolve_path_under_base(&base, "../outside").is_err());
    }

    #[test]
    fn confine_user_path_relative() {
        let dir = TempDir::new().unwrap();
        let base = dir.path().join("files");
        fs::create_dir_all(&base).unwrap();
        let p = confine_user_path_under_base(&base, "data/out.parquet").unwrap();
        assert!(p.starts_with(base.canonicalize().unwrap()));
    }

    #[test]
    fn confine_user_path_rejects_escape() {
        let dir = TempDir::new().unwrap();
        let base = dir.path().join("files");
        fs::create_dir_all(&base).unwrap();
        assert!(confine_user_path_under_base(&base, "../outside").is_err());
    }
}
