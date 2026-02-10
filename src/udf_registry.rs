//! UDF registry: session-scoped storage for Rust and Python UDFs.
//! PySpark parity: register_udf / spark.udf.register; call_udf resolves by name.

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

/// Python UDF kind (scalar vs vectorized).
#[cfg(feature = "pyo3")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PythonUdfKind {
    /// Row-at-a-time Python UDF (existing behavior).
    Scalar,
    /// Vectorized UDF that expects column batches.
    Vectorized,
    /// Grouped, vectorized aggregation UDF (one result per group).
    ///
    /// Registered via pandas_udf(function_type="grouped_agg") or similar Python API.
    /// Executed in a groupBy().agg(...) context, not via the generic execute_python_udf path.
    GroupedVectorizedAgg,
}

/// Python UDF entry stored in the registry.
#[cfg(feature = "pyo3")]
pub struct PythonUdfEntry {
    pub callable: pyo3::Py<pyo3::PyAny>,
    pub return_type: DataType,
    pub kind: PythonUdfKind,
}

/// Session-scoped UDF registry. Rust UDFs run lazily via Polars Expr::map.
/// Python UDF slots (when pyo3 enabled) hold name -> (callable, return_type, kind).
#[derive(Clone)]
pub struct UdfRegistry {
    rust_udfs: Arc<std::sync::RwLock<HashMap<String, Arc<dyn RustUdf>>>>,
    #[cfg(feature = "pyo3")]
    python_udfs: Arc<std::sync::RwLock<HashMap<String, Arc<PythonUdfEntry>>>>,
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self {
            rust_udfs: Arc::new(std::sync::RwLock::new(HashMap::new())),
            #[cfg(feature = "pyo3")]
            python_udfs: Arc::new(std::sync::RwLock::new(HashMap::new())),
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
    pub fn get_rust_udf(&self, name: &str, case_sensitive: bool) -> Option<Arc<dyn RustUdf>> {
        let guard = self.rust_udfs.read().ok()?;
        if case_sensitive {
            guard.get(name).cloned()
        } else {
            let name_lower = name.to_lowercase();
            guard
                .iter()
                .find(|(k, _)| k.to_lowercase() == name_lower)
                .map(|(_, v)| v.clone())
        }
    }

    /// Check if a UDF (Rust or Python) exists.
    #[allow(dead_code)] // used by SQL translator and Python
    pub fn has_udf(&self, name: &str, case_sensitive: bool) -> bool {
        if self.get_rust_udf(name, case_sensitive).is_some() {
            return true;
        }
        #[cfg(feature = "pyo3")]
        {
            self.get_python_udf(name, case_sensitive).is_some()
        }
        #[cfg(not(feature = "pyo3"))]
        false
    }

    /// Register a scalar (row-at-a-time) Python UDF.
    #[cfg(feature = "pyo3")]
    pub fn register_python_udf(
        &self,
        name: &str,
        callable: pyo3::Py<pyo3::PyAny>,
        return_type: DataType,
    ) -> Result<(), PolarsError> {
        self.register_python_udf_with_kind(name, callable, return_type, PythonUdfKind::Scalar)
    }

    /// Register a vectorized Python UDF (column batches).
    #[cfg(feature = "pyo3")]
    pub fn register_vectorized_python_udf(
        &self,
        name: &str,
        callable: pyo3::Py<pyo3::PyAny>,
        return_type: DataType,
    ) -> Result<(), PolarsError> {
        self.register_python_udf_with_kind(name, callable, return_type, PythonUdfKind::Vectorized)
    }

    /// Register a grouped, vectorized aggregation Python UDF (GROUPED_AGG-style).
    ///
    /// This UDF is intended for use in groupBy().agg(...) only and will be
    /// evaluated once per group, returning exactly one value per group.
    #[cfg(feature = "pyo3")]
    pub fn register_grouped_vectorized_python_udf(
        &self,
        name: &str,
        callable: pyo3::Py<pyo3::PyAny>,
        return_type: DataType,
    ) -> Result<(), PolarsError> {
        self.register_python_udf_with_kind(
            name,
            callable,
            return_type,
            PythonUdfKind::GroupedVectorizedAgg,
        )
    }

    /// Internal helper to register any Python UDF kind.
    #[cfg(feature = "pyo3")]
    fn register_python_udf_with_kind(
        &self,
        name: &str,
        callable: pyo3::Py<pyo3::PyAny>,
        return_type: DataType,
        kind: PythonUdfKind,
    ) -> Result<(), PolarsError> {
        let entry = PythonUdfEntry {
            callable,
            return_type,
            kind,
        };
        self.python_udfs
            .write()
            .map_err(|_| PolarsError::ComputeError("udf registry lock poisoned".into()))?
            .insert(name.to_string(), Arc::new(entry));
        Ok(())
    }

    #[cfg(feature = "pyo3")]
    pub fn get_python_udf(&self, name: &str, case_sensitive: bool) -> Option<Arc<PythonUdfEntry>> {
        let guard = self.python_udfs.read().ok()?;
        if case_sensitive {
            guard.get(name).cloned()
        } else {
            let name_lower = name.to_lowercase();
            guard
                .iter()
                .find(|(k, _)| k.to_lowercase() == name_lower)
                .map(|(_, v)| v.clone())
        }
    }
}
