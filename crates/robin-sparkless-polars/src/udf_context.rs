//! Thread-local UDF context for call_udf. Set by the main crate's SparkSession.

use std::cell::RefCell;
use std::sync::Arc;

use crate::udf_registry::UdfRegistry;

thread_local! {
    static THREAD_UDF_CONTEXT: RefCell<Option<(Arc<UdfRegistry>, bool)>> = const { RefCell::new(None) };
}

/// Set the thread-local UDF context (registry + case_sensitive). Called by the main crate's session.
pub fn set_thread_udf_context(registry: Arc<UdfRegistry>, case_sensitive: bool) {
    THREAD_UDF_CONTEXT.with(|cell| *cell.borrow_mut() = Some((registry, case_sensitive)));
}

/// Get the thread-local UDF context for call_udf.
pub fn get_thread_udf_context() -> Option<(Arc<UdfRegistry>, bool)> {
    THREAD_UDF_CONTEXT.with(|cell| cell.borrow().clone())
}
