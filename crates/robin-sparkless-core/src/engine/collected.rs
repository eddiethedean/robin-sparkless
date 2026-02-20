//! Row type produced by DataFrameBackend::collect (engine-agnostic).

use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Rows as list of maps: column name -> JSON value. Used by bindings and collect.
pub type CollectedRows = Vec<HashMap<String, JsonValue>>;
