use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const CONTROL_PROTOCOL_VERSION: u32 = 1;
pub const HTTP_AGENT_VERSION: u32 = 1;
pub const MAX_CONTROL_FRAME_BYTES: usize = 16 * 1024;
pub const MAX_ERROR_BYTES: usize = 4096;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatementRequest {
    pub session: String,
    pub kind: String,
    pub code: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseRequest {
    pub session: String,
}

#[derive(Debug, Serialize)]
pub struct ControlRecord<'a> {
    #[serde(rename = "type")]
    pub kind: &'a str,
    pub protocol_version: u32,
    pub agent_version: u32,
    pub binary_version: &'a str,
    pub launch_id: &'a str,
    pub http_url: &'a str,
    pub capabilities: &'a [&'a str],
}

pub fn success_envelope(schema: Value, rows: Vec<Vec<Value>>) -> Value {
    json!({
        "status": "ok",
        "execution_count": 1,
        "data": {
            "application/json": {
                "schema": schema,
                "data": rows
            }
        }
    })
}

pub fn error_envelope(name: &str, message: &str) -> Value {
    json!({
        "status": "error",
        "ename": name,
        "evalue": truncate_utf8(message, MAX_ERROR_BYTES),
        "execution_count": 1
    })
}

pub fn transport_error(name: &str, message: &str) -> Value {
    json!({
        "error": name,
        "message": truncate_utf8(message, MAX_ERROR_BYTES)
    })
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes.saturating_sub(3);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &value[..end])
}
