//! Node.js bindings for robin-sparkless (napi-rs).

#![allow(clippy::needless_borrow)]

use napi_derive::napi;
use napi::bindgen_prelude::{Array, Env, Error, Object, Result};
use napi::{JsNumber, JsObject, JsString, JsUnknown};
use robin_sparkless::{DataFrame, SparkSession, SparkSessionBuilder};
use serde_json::Value as JsonValue;
use std::sync::Arc;

// -----------------------------------------------------------------------------
// SparkSessionBuilder
// -----------------------------------------------------------------------------

#[napi]
pub struct NodeSparkSessionBuilder {
    inner: SparkSessionBuilder,
}

#[napi]
impl NodeSparkSessionBuilder {
    #[napi(js_name = "appName")]
    pub fn app_name(&mut self, name: String) -> &Self {
        self.inner = self.inner.clone().app_name(name);
        self
    }

    #[napi(js_name = "getOrCreate")]
    pub fn get_or_create(&self) -> NodeSparkSession {
        NodeSparkSession {
            inner: self.inner.clone().get_or_create(),
        }
    }
}

/// Returns SparkSession.builder() (Node API).
#[napi]
pub fn builder() -> NodeSparkSessionBuilder {
    NodeSparkSessionBuilder {
        inner: SparkSession::builder(),
    }
}

// -----------------------------------------------------------------------------
// SparkSession
// -----------------------------------------------------------------------------

#[napi]
pub struct NodeSparkSession {
    inner: SparkSession,
}

#[napi]
impl NodeSparkSession {
    /// createDataFrame(rows, columnNames): rows = array of [col0, col1, col2] (number, number, string);
    /// columnNames = exactly 3 names e.g. ["id", "age", "name"].
    #[napi(js_name = "createDataFrame")]
    pub fn create_dataframe(
        &self,
        _env: Env,
        rows: Array,
        column_names: Vec<String>,
    ) -> Result<NodeDataFrame> {
        let len = rows.len();
        let mut data: Vec<(i64, i64, String)> = Vec::with_capacity(len as usize);
        for i in 0..len {
            let row_arr = rows.get::<Array>(i)?.ok_or_else(|| {
                Error::from_reason("createDataFrame: each row must be an array [col0, col1, col2]")
            })?;
            if row_arr.len() < 3 {
                return Err(Error::from_reason(
                    "createDataFrame: each row must have 3 elements [number, number, string]",
                ));
            }
            let v0 = row_arr.get::<JsNumber>(0)?.ok_or_else(|| Error::from_reason("row[0] number"))?;
            let v1 = row_arr.get::<JsNumber>(1)?.ok_or_else(|| Error::from_reason("row[1] number"))?;
            let v2 = row_arr.get::<JsString>(2)?.ok_or_else(|| Error::from_reason("row[2] string"))?;
            let n0 = v0.get_int64()?;
            let n1 = v1.get_int64()?;
            let s2 = v2.into_utf8()?.as_str()?.to_owned();
            data.push((n0, n1, s2));
        }
        let names_ref: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .create_dataframe(data, names_ref)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NodeDataFrame { inner: Arc::new(df) })
    }
}

// -----------------------------------------------------------------------------
// DataFrame
// -----------------------------------------------------------------------------

#[napi]
pub struct NodeDataFrame {
    inner: Arc<DataFrame>,
}

#[napi]
impl NodeDataFrame {
    /// Collect as array of objects (column name -> value).
    #[napi]
    pub fn collect(&self, env: Env) -> Result<Object> {
        let rows = self
            .inner
            .collect_as_json_rows()
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let mut arr = env.create_array_with_length(rows.len())?;
        for (i, row) in rows.into_iter().enumerate() {
            let mut obj = env.create_object()?;
            for (k, v) in row {
                let js_val = json_value_to_js(&env, &v)?;
                obj.set_named_property(&k, js_val)?;
            }
            arr.set_element(i as u32, obj)?;
        }
        Ok(arr)
    }
}

fn json_value_to_js(env: &Env, v: &JsonValue) -> Result<JsUnknown> {
    let val = match v {
        JsonValue::Null => env.get_null()?.into_unknown(),
        JsonValue::Bool(b) => env.get_boolean(*b)?.into_unknown(),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                env.create_int64(i)?.into_unknown()
            } else if let Some(u) = n.as_u64() {
                env.create_int64(u as i64)?.into_unknown()
            } else if let Some(f) = n.as_f64() {
                env.create_double(f)?.into_unknown()
            } else {
                env.get_null()?.into_unknown()
            }
        }
        JsonValue::String(s) => env.create_string_from_std(s.clone())?.into_unknown(),
        _ => env.get_null()?.into_unknown(),
    };
    Ok(val)
}
