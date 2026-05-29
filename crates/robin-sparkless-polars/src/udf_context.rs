//! Thread-local UDF context for call_udf. Set by the main crate's SparkSession.

use std::cell::RefCell;
use std::sync::Arc;

use robin_sparkless_core::SessionRuntimeConfig;

use crate::udf_registry::UdfRegistry;

thread_local! {
    static THREAD_UDF_CONTEXT: RefCell<Option<UdfContext>> = const { RefCell::new(None) };
}

/// Context set per thread when executing with a SparkSession (registry, case_sensitive, session timezone, runtime config).
#[derive(Clone)]
pub struct UdfContext {
    pub registry: Arc<UdfRegistry>,
    pub case_sensitive: bool,
    /// Session timezone for hour/minute/second extraction (e.g. "UTC"). Default "UTC".
    pub session_time_zone: Option<String>,
    pub runtime_config: SessionRuntimeConfig,
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
    set_thread_udf_context_full(
        registry,
        case_sensitive,
        session_time_zone,
        SessionRuntimeConfig::default(),
    );
}

/// Full thread context including PySpark compat / ANSI settings.
pub fn set_thread_udf_context_full(
    registry: Arc<UdfRegistry>,
    case_sensitive: bool,
    session_time_zone: Option<String>,
    runtime_config: SessionRuntimeConfig,
) {
    THREAD_UDF_CONTEXT.with(|cell| {
        *cell.borrow_mut() = Some(UdfContext {
            registry,
            case_sensitive,
            session_time_zone,
            runtime_config,
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

/// Current session runtime config for this thread (ANSI, map flags).
pub fn get_thread_runtime_config() -> SessionRuntimeConfig {
    THREAD_UDF_CONTEXT.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|ctx| ctx.runtime_config.clone())
            .unwrap_or_default()
    })
}

/// Whether ANSI SQL mode is enabled for the current thread.
pub fn get_thread_ansi_enabled() -> bool {
    get_thread_runtime_config().ansi_enabled
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

/// Update runtime config in thread context (after spark.conf.set).
pub fn update_thread_runtime_config(runtime_config: SessionRuntimeConfig) {
    THREAD_UDF_CONTEXT.with(|cell| {
        if let Some(ref mut ctx) = *cell.borrow_mut() {
            ctx.runtime_config = runtime_config;
        }
    });
}

/// Clear the thread-local UDF context. Called when session.stop() is invoked.
pub fn clear_thread_udf_context() {
    THREAD_UDF_CONTEXT.with(|cell| *cell.borrow_mut() = None);
}
