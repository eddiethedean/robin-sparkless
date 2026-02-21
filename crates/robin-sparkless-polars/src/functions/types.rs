//! Type name parsing for cast/schema.

use polars::prelude::{DataType, TimeUnit};

/// Parse PySpark-like type name to Polars DataType.
/// Decimal(precision, scale) is mapped to Float64 for schema parity (Polars dtype-decimal not enabled).
pub fn parse_type_name(name: &str) -> Result<DataType, String> {
    let s = name.trim().to_lowercase();
    if s.starts_with("decimal(") && s.contains(')') {
        return Ok(DataType::Float64);
    }
    Ok(match s.as_str() {
        "int" | "integer" => DataType::Int32,
        "long" | "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "string" | "str" => DataType::String,
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date,
        "timestamp" => DataType::Datetime(TimeUnit::Microseconds, None),
        _ => return Err(format!("unknown type name: {name}")),
    })
}
