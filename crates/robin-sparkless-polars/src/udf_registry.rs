//! UDF registry: session-scoped storage for Rust UDFs.
//! PySpark parity: register_udf; call_udf resolves by name.

#[allow(unused_imports)]
use polars::prelude::{DataType, PolarsError, Series};
use std::collections::HashMap;
use std::sync::Arc;

/// Rust UDF: takes columns as Series, returns one Series. Used via Expr::map / map_many.
pub trait RustUdf: Send + Sync {
    fn apply(&self, columns: &[Series]) -> Result<Series, PolarsError>;
}

/// Type-erased wrapper for Rust UDF closures.
struct RustUdfWrapper<F>
where
    F: Fn(&[Series]) -> Result<Series, PolarsError> + Send + Sync,
{
    f: F,
}

impl<F> RustUdf for RustUdfWrapper<F>
where
    F: Fn(&[Series]) -> Result<Series, PolarsError> + Send + Sync,
{
    fn apply(&self, columns: &[Series]) -> Result<Series, PolarsError> {
        (self.f)(columns)
    }
}

/// Session-scoped UDF registry. Rust UDFs run lazily via Polars Expr::map.
#[derive(Clone)]
pub struct UdfRegistry {
    rust_udfs: Arc<std::sync::RwLock<HashMap<String, Arc<dyn RustUdf>>>>,
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self {
            rust_udfs: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a Rust UDF. Runs lazily when used in DataFrame operations.
    pub fn register_rust_udf<F>(&self, name: &str, f: F) -> Result<(), PolarsError>
    where
        F: Fn(&[Series]) -> Result<Series, PolarsError> + Send + Sync + 'static,
    {
        let wrapper = Arc::new(RustUdfWrapper { f });
        self.rust_udfs
            .write()
            .map_err(|_| PolarsError::ComputeError("udf registry lock poisoned".into()))?
            .insert(name.to_string(), wrapper);
        Ok(())
    }

    /// Look up a Rust UDF by name. Case sensitivity follows session config.
    /// Returns `Err` if the registry lock is poisoned (e.g. a thread panicked while holding it).
    pub fn get_rust_udf(
        &self,
        name: &str,
        case_sensitive: bool,
    ) -> Result<Option<Arc<dyn RustUdf>>, PolarsError> {
        let guard = self
            .rust_udfs
            .read()
            .map_err(|_| PolarsError::ComputeError("udf registry lock poisoned".into()))?;
        Ok(if case_sensitive {
            guard.get(name).cloned()
        } else {
            let name_lower = name.to_lowercase();
            guard
                .iter()
                .find(|(k, _)| k.to_lowercase() == name_lower)
                .map(|(_, v)| v.clone())
        })
    }

    /// Check if a Rust UDF exists. Returns `Err` if the registry lock is poisoned.
    #[allow(dead_code)] // used by SQL translator
    pub fn has_udf(&self, name: &str, case_sensitive: bool) -> Result<bool, PolarsError> {
        self.get_rust_udf(name, case_sensitive).map(|o| o.is_some())
    }

    /// Clear all registered UDFs (used by SparkSession.stop()).
    pub fn clear(&self) -> Result<(), PolarsError> {
        self.rust_udfs
            .write()
            .map_err(|_| PolarsError::ComputeError("udf registry lock poisoned".into()))?
            .clear();
        Ok(())
    }
}
