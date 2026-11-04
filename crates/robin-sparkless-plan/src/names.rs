use robin_sparkless_core::{DataType, EngineError};

/// Resolve a name using Spark-style configurable case sensitivity and report ambiguous matches.
pub fn resolve_name<'a>(
    names: &'a [String],
    requested: &str,
    case_sensitive: bool,
) -> Result<&'a str, EngineError> {
    let matches = matching_indices(names, requested, case_sensitive);
    match matches.as_slice() {
        [index] => Ok(names[*index].as_str()),
        [] => Err(EngineError::not_found(format!(
            "column '{requested}' does not exist"
        ))),
        _ => Err(EngineError::invalid_input(format!(
            "column '{requested}' is ambiguous"
        ))),
    }
}

pub fn matching_indices(names: &[String], requested: &str, case_sensitive: bool) -> Vec<usize> {
    names
        .iter()
        .enumerate()
        .filter(|(_, name)| {
            if case_sensitive {
                name.as_str() == requested
            } else {
                name.to_lowercase() == requested.to_lowercase()
            }
        })
        .map(|(index, _)| index)
        .collect()
}

/// Match Connect identifiers with the ASCII-only rules its resolver has historically used.
pub fn matching_indices_ascii_case_insensitive(names: &[String], requested: &str) -> Vec<usize> {
    names
        .iter()
        .enumerate()
        .filter(|(_, name)| name.eq_ignore_ascii_case(requested))
        .map(|(index, _)| index)
        .collect()
}

/// Return the shared Spark numeric precedence result used by SQL/Connect analysis.
/// Callers validate that both inputs are numeric before using this helper.
pub fn common_numeric_type(left: &DataType, right: &DataType) -> DataType {
    match (left, right) {
        (DataType::Double, _) | (_, DataType::Double) => DataType::Double,
        (DataType::Float, _) | (_, DataType::Float) => DataType::Float,
        (DataType::Long, _) | (_, DataType::Long) => DataType::Long,
        (DataType::Integer, _) | (_, DataType::Integer) => DataType::Integer,
        (DataType::Short, _) | (_, DataType::Short) => DataType::Short,
        (DataType::Byte, DataType::Byte) => DataType::Byte,
        (DataType::Decimal(precision, scale), _) | (_, DataType::Decimal(precision, scale)) => {
            DataType::Decimal(*precision, *scale)
        }
        _ => left.clone(),
    }
}
