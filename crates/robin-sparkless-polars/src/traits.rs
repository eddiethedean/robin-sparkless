//! Traits for idiomatic conversion to and from robin-sparkless DataFrames.

use crate::dataframe::DataFrame;
use crate::error::{EngineError, polars_to_core_error};
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
            .map_err(polars_to_core_error)
    }
}

impl IntoRobinDf for Vec<(i64, String)> {
    fn into_robin_df(self, session: &SparkSession) -> Result<DataFrame, EngineError> {
        let rows: Vec<Vec<JsonValue>> = self
            .into_iter()
            .map(|(a, b)| vec![JsonValue::Number(a.into()), JsonValue::String(b)])
            .collect();
        let schema = vec![
            ("c0".to_string(), "bigint".to_string()),
            ("c1".to_string(), "string".to_string()),
        ];
        session.create_dataframe_from_rows_engine(rows, schema)
    }
}

impl IntoRobinDf for Vec<(i64, i64, i64, String)> {
    fn into_robin_df(self, session: &SparkSession) -> Result<DataFrame, EngineError> {
        let rows: Vec<Vec<JsonValue>> = self
            .into_iter()
            .map(|(a, b, c, d)| {
                vec![
                    JsonValue::Number(a.into()),
                    JsonValue::Number(b.into()),
                    JsonValue::Number(c.into()),
                    JsonValue::String(d),
                ]
            })
            .collect();
        let schema = vec![
            ("c0".to_string(), "bigint".to_string()),
            ("c1".to_string(), "bigint".to_string()),
            ("c2".to_string(), "bigint".to_string()),
            ("c3".to_string(), "string".to_string()),
        ];
        session.create_dataframe_from_rows_engine(rows, schema)
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
        df.collect_as_json_rows_engine()
    }
}

/// Rows as arrays of values in column order (order from [`DataFrame::columns`]).
impl FromRobinDf for Vec<Vec<JsonValue>> {
    fn from_robin_df(df: &DataFrame) -> Result<Self, EngineError> {
        let names = df.columns_engine()?;
        let rows = df.collect_as_json_rows_engine()?;
        Ok(rows
            .into_iter()
            .map(|row| names.iter().map(|n| row[n].clone()).collect())
            .collect())
    }
}
