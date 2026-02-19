//! Traits for idiomatic conversion to and from robin-sparkless DataFrames.

use crate::dataframe::DataFrame;
use crate::error::EngineError;
use crate::session::SparkSession;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Convert a value into a [`DataFrame`] using the given session.
pub trait IntoRobinDf {
    fn into_robin_df(self, session: &SparkSession) -> Result<DataFrame, EngineError>;
}

impl IntoRobinDf for Vec<(i64, i64, String)> {
    fn into_robin_df(self, session: &SparkSession) -> Result<DataFrame, EngineError> {
        session
            .create_dataframe(self, vec!["c0", "c1", "c2"])
            .map_err(Into::into)
    }
}

/// Convert a [`DataFrame`] into a value (e.g. JSON rows for bindings).
pub trait FromRobinDf {
    fn from_robin_df(df: &DataFrame) -> Result<Self, EngineError>
    where
        Self: Sized;
}

impl FromRobinDf for Vec<HashMap<String, JsonValue>> {
    fn from_robin_df(df: &DataFrame) -> Result<Self, EngineError> {
        df.collect_as_json_rows().map_err(Into::into)
    }
}
