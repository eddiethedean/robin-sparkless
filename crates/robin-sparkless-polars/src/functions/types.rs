//! Type name parsing for cast/schema.

use polars::prelude::{DataType, TimeUnit};

/// Parse PySpark-like type name to Polars DataType.
/// Decimal(precision, scale) and bare "decimal" are mapped to Float64 for schema parity (Polars dtype-decimal not enabled; #853).
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
        "decimal" => DataType::Float64, // #853: bare "decimal" -> Float64 (no native decimal dtype)
        "string" | "str" => DataType::String,
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date,
        "timestamp" | "datetime" | "timestamp_ntz" => {
            DataType::Datetime(TimeUnit::Microseconds, None)
        }
        _ => return Err(format!("unknown type name: {name}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_type_name_int_long_float_double() {
        assert!(matches!(parse_type_name("int"), Ok(DataType::Int32)));
        assert!(matches!(parse_type_name("integer"), Ok(DataType::Int32)));
        assert!(matches!(parse_type_name("long"), Ok(DataType::Int64)));
        assert!(matches!(parse_type_name("bigint"), Ok(DataType::Int64)));
        assert!(matches!(parse_type_name("float"), Ok(DataType::Float32)));
        assert!(matches!(parse_type_name("double"), Ok(DataType::Float64)));
    }

    #[test]
    fn parse_type_name_string_bool_date_timestamp() {
        assert!(matches!(parse_type_name("string"), Ok(DataType::String)));
        assert!(matches!(parse_type_name("str"), Ok(DataType::String)));
        assert!(matches!(parse_type_name("boolean"), Ok(DataType::Boolean)));
        assert!(matches!(parse_type_name("bool"), Ok(DataType::Boolean)));
        assert!(matches!(parse_type_name("date"), Ok(DataType::Date)));
        assert!(matches!(
            parse_type_name("timestamp"),
            Ok(DataType::Datetime(_, None))
        ));
        assert!(matches!(
            parse_type_name("datetime"),
            Ok(DataType::Datetime(_, None))
        ));
        assert!(matches!(
            parse_type_name("timestamp_ntz"),
            Ok(DataType::Datetime(_, None))
        ));
    }

    #[test]
    fn parse_type_name_decimal_and_unknown() {
        assert!(matches!(parse_type_name("decimal"), Ok(DataType::Float64)));
        assert!(matches!(
            parse_type_name("decimal(10,2)"),
            Ok(DataType::Float64)
        ));
        assert!(parse_type_name("unknown_type").is_err());
        assert!(parse_type_name("").is_err());
    }

    #[test]
    fn parse_type_name_trim_whitespace() {
        assert!(matches!(parse_type_name("  long  "), Ok(DataType::Int64)));
    }
}
