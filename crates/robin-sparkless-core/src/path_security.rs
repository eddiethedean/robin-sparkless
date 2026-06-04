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
}
