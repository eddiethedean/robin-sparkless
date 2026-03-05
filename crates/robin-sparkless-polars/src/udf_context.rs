//! Thread-local UDF context for call_udf. Set by the main crate's SparkSession.

use std::cell::RefCell;
use std::sync::Arc;

use crate::udf_registry::UdfRegistry;

thread_local! {
    static THREAD_UDF_CONTEXT: RefCell<Option<UdfContext>> = const { RefCell::new(None) };
}

/// Context set per thread when executing with a SparkSession (registry, case_sensitive, session timezone).
#[derive(Clone)]
pub struct UdfContext {
    pub registry: Arc<UdfRegistry>,
    pub case_sensitive: bool,
    /// Session timezone for hour/minute/second extraction (e.g. "UTC"). Default "UTC".
    pub session_time_zone: Option<String>,
}

/// Set the thread-local UDF context (registry + case_sensitive). Called by the main crate's session.
pub fn set_thread_udf_context(registry: Arc<UdfRegistry>, case_sensitive: bool) {
    set_thread_udf_context_with_tz(registry, case_sensitive, None);
}

/// Set the thread-local UDF context including session timezone (for hour/minute/second; #1154).
pub fn set_thread_udf_context_with_tz(
    registry: Arc<UdfRegistry>,
    case_sensitive: bool,
    session_time_zone: Option<String>,
) {
    THREAD_UDF_CONTEXT.with(|cell| {
        *cell.borrow_mut() = Some(UdfContext {
            registry,
            case_sensitive,
            session_time_zone,
        })
    });
}

/// Get the thread-local UDF context for call_udf.
pub fn get_thread_udf_context() -> Option<(Arc<UdfRegistry>, bool)> {
    THREAD_UDF_CONTEXT.with(|cell| {
        cell.borrow()
            .clone()
            .map(|ctx| (ctx.registry, ctx.case_sensitive))
    })
}

/// Get session timezone from thread context (e.g. "UTC"). Default "UTC" when not set.
pub fn get_thread_session_time_zone() -> String {
    THREAD_UDF_CONTEXT.with(|cell| {
        cell.borrow()
            .as_ref()
            .and_then(|ctx| ctx.session_time_zone.as_deref())
            .unwrap_or("UTC")
            .to_string()
    })
}

/// Update the session timezone in the thread context (#1154). Called when spark.conf.set("spark.sql.session.timeZone", ...) is used.
pub fn update_thread_session_time_zone(tz: Option<String>) {
    THREAD_UDF_CONTEXT.with(|cell| {
        if let Some(ref mut ctx) = *cell.borrow_mut() {
            ctx.session_time_zone = tz;
        }
    });
}
