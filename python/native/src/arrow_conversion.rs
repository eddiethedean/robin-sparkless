//! Arrow schema and value conversion at the Python boundary.

use super::*;

pub(super) fn arrow_type_to_spark_type(arrow_type: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    let py = arrow_type.py();
    let type_name = arrow_type.str()?.to_string();
    let arrow_types = PyModule::import(py, "pyarrow.types")?;
    let is_type = |predicate: &str| -> PyResult<bool> {
        arrow_types
            .getattr(predicate)?
            .call1((arrow_type,))?
            .extract()
    };

    if is_type("is_decimal")? {
        let precision: usize = arrow_type.getattr("precision")?.extract()?;
        let scale: usize = arrow_type.getattr("scale")?.extract()?;
        return Ok(Some(format!("decimal({precision},{scale})")));
    }
    if is_type("is_list")? || is_type("is_large_list")? || is_type("is_fixed_size_list")? {
        let value_type = arrow_type.getattr("value_type")?;
        return Ok(
            arrow_type_to_spark_type(&value_type)?.map(|value_type| format!("array<{value_type}>"))
        );
    }
    if is_type("is_struct")? {
        let mut fields = Vec::new();
        for field in arrow_type.try_iter()? {
            let field = field?;
            let name: String = field.getattr("name")?.extract()?;
            let field_type = field.getattr("type")?;
            let Some(field_type) = arrow_type_to_spark_type(&field_type)? else {
                return Ok(None);
            };
            // Struct type strings use backtick-quoted identifiers so field names
            // containing commas, colons, or angle brackets remain unambiguous.
            let quoted_name = name.replace('`', "``");
            fields.push(format!("`{quoted_name}`:{field_type}"));
        }
        return Ok(Some(format!("struct<{}>", fields.join(","))));
    }
    if is_type("is_map")? {
        let key_type = arrow_type.getattr("key_type")?;
        let item_type = arrow_type.getattr("item_type")?;
        let (Some(key_type), Some(item_type)) = (
            arrow_type_to_spark_type(&key_type)?,
            arrow_type_to_spark_type(&item_type)?,
        ) else {
            return Ok(None);
        };
        return Ok(Some(format!("map<{key_type},{item_type}>")));
    }
    if is_type("is_dictionary")? {
        let value_type = arrow_type.getattr("value_type")?;
        return arrow_type_to_spark_type(&value_type);
    }

    Ok(Some(match type_name.as_str() {
        "bool" => "boolean".to_string(),
        "int8" => "int".to_string(),
        "int16" => "smallint".to_string(),
        "int32" => "int32".to_string(),
        "uint8" | "uint16" | "uint32" | "int64" => "bigint".to_string(),
        "uint64" => "decimal(20,0)".to_string(),
        "float" | "double" => "double".to_string(),
        "string" | "large_string" => "string".to_string(),
        "date32[day]" | "date64[ms]" => "date".to_string(),
        "binary" | "large_binary" => "binary".to_string(),
        ty if ty.starts_with("timestamp[") => "timestamp".to_string(),
        _ => return Ok(None),
    }))
}

pub(super) fn is_pyarrow_table_or_batch(data: &Bound<'_, PyAny>) -> bool {
    let ty = data.get_type();
    let module = ty
        .getattr("__module__")
        .and_then(|value| value.extract::<String>())
        .unwrap_or_default();
    let name = ty
        .getattr("__name__")
        .and_then(|value| value.extract::<String>())
        .unwrap_or_default();
    module.starts_with("pyarrow") && (name == "Table" || name == "RecordBatch")
}

pub(super) fn pyarrow_is_type(
    arrow_types: &Bound<'_, PyModule>,
    arrow_type: &Bound<'_, PyAny>,
    predicate: &str,
) -> PyResult<bool> {
    arrow_types
        .getattr(predicate)?
        .call1((arrow_type,))?
        .extract()
}

pub(super) fn normalize_arrow_value(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    arrow_type: &Bound<'_, PyAny>,
    arrow_types: &Bound<'_, PyModule>,
) -> PyResult<Py<PyAny>> {
    if value.is_none() {
        return Ok(value.clone().unbind());
    }
    // Python integers above i64::MAX fall through the generic JSON conversion
    // to f64, which cannot preserve all UInt64 values. Route Arrow UInt64 through
    // Decimal so the decimal(20,0) schema keeps every source digit.
    if pyarrow_is_type(arrow_types, arrow_type, "is_uint64")? {
        let decimal = PyModule::import(py, "decimal")?.getattr("Decimal")?;
        return decimal.call1((value,))?.into_py_any(py);
    }
    if pyarrow_is_type(arrow_types, arrow_type, "is_dictionary")? {
        let value_type = arrow_type.getattr("value_type")?;
        return normalize_arrow_value(py, value, &value_type, arrow_types);
    }
    if pyarrow_is_type(arrow_types, arrow_type, "is_map")? {
        let key_type = arrow_type.getattr("key_type")?;
        let item_type = arrow_type.getattr("item_type")?;
        let entries = if value.cast::<PyDict>().is_ok() {
            value.call_method0("items")?
        } else {
            value.clone()
        };
        let mapped = PyDict::new(py);
        let mut seen_nan_key = false;
        for entry in entries.try_iter()? {
            let entry = entry?;
            let pair = entry.cast::<PyTuple>()?;
            if pair.len() != 2 {
                continue;
            }
            let key = pair.get_item(0)?;
            let item = pair.get_item(1)?;
            let key = normalize_arrow_value(py, &key, &key_type, arrow_types)?;
            let item = normalize_arrow_value(py, &item, &item_type, arrow_types)?;
            let key_is_nan = key
                .bind(py)
                .get_type()
                .name()
                .is_ok_and(|name| name == "float")
                && key
                    .bind(py)
                    .extract::<f64>()
                    .is_ok_and(|value| value.is_nan());
            if (key_is_nan && seen_nan_key)
                || (!key_is_nan && mapped.get_item(key.bind(py))?.is_some())
            {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "PyArrow map contains duplicate keys; createDataFrame cannot preserve duplicate map keys",
                ));
            }
            seen_nan_key |= key_is_nan;
            mapped.set_item(key.bind(py), item.bind(py))?;
        }
        return mapped.into_py_any(py);
    }
    if pyarrow_is_type(arrow_types, arrow_type, "is_list")?
        || pyarrow_is_type(arrow_types, arrow_type, "is_large_list")?
        || pyarrow_is_type(arrow_types, arrow_type, "is_fixed_size_list")?
    {
        let item_type = arrow_type.getattr("value_type")?;
        let items = PyList::empty(py);
        for item in value.try_iter()? {
            let item = item?;
            let normalized = normalize_arrow_value(py, &item, &item_type, arrow_types)?;
            items.append(normalized.bind(py))?;
        }
        return items.into_py_any(py);
    }
    if pyarrow_is_type(arrow_types, arrow_type, "is_struct")? {
        if let Ok(object) = value.cast::<PyDict>() {
            let normalized = PyDict::new(py);
            for field in arrow_type.try_iter()? {
                let field = field?;
                let name: String = field.getattr("name")?.extract()?;
                let field_type = field.getattr("type")?;
                if let Some(field_value) = object.get_item(name.as_str())? {
                    let field_value =
                        normalize_arrow_value(py, &field_value, &field_type, arrow_types)?;
                    normalized.set_item(name, field_value.bind(py))?;
                }
            }
            return normalized.into_py_any(py);
        }
    }
    Ok(value.clone().unbind())
}

// JSON-quoted field segments distinguish literal dots/brackets from nesting.
pub(super) fn arrow_field_path(parent: &str, name: &str) -> String {
    let field = serde_json::to_string(name).expect("field names serialize as JSON strings");
    if parent.is_empty() {
        field
    } else {
        format!("{parent}.{field}")
    }
}

pub(super) fn split_top_level_spark_type_parts(value: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut angle_depth = 0i32;
    let mut paren_depth = 0i32;
    let mut in_backticks = false;
    let mut chars = value.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if ch == '`' {
            if in_backticks && chars.peek().is_some_and(|(_, next)| *next == '`') {
                chars.next();
            } else {
                in_backticks = !in_backticks;
            }
            continue;
        }
        if in_backticks {
            continue;
        }
        match ch {
            '<' => angle_depth += 1,
            '>' => angle_depth -= 1,
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            ',' if angle_depth == 0 && paren_depth == 0 => {
                parts.push(value[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(value[start..].trim());
    parts
}

pub(super) fn split_spark_struct_fields(type_name: &str) -> Vec<(String, &str)> {
    let type_name = type_name.trim();
    let lower = type_name.to_ascii_lowercase();
    if !lower.starts_with("struct<") || !type_name.ends_with('>') {
        return Vec::new();
    }

    let fields = &type_name[7..type_name.len() - 1];
    let mut parsed = Vec::new();
    for field in split_top_level_spark_type_parts(fields) {
        let mut angle_depth = 0i32;
        let mut paren_depth = 0i32;
        let mut in_backticks = false;
        let mut chars = field.char_indices().peekable();
        let mut separator = None;
        while let Some((index, ch)) = chars.next() {
            if ch == '`' {
                if in_backticks && chars.peek().is_some_and(|(_, next)| *next == '`') {
                    chars.next();
                } else {
                    in_backticks = !in_backticks;
                }
                continue;
            }
            if in_backticks {
                continue;
            }
            match ch {
                '<' => angle_depth += 1,
                '>' => angle_depth -= 1,
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                ':' if angle_depth == 0 && paren_depth == 0 => {
                    separator = Some(index);
                    break;
                }
                _ => {}
            }
        }
        let Some(separator) = separator else {
            continue;
        };
        let encoded_name = field[..separator].trim();
        let field_name = encoded_name
            .strip_prefix('`')
            .and_then(|name| name.strip_suffix('`'))
            .map(|name| name.replace("``", "`"))
            .unwrap_or_else(|| encoded_name.to_string());
        parsed.push((field_name, field[separator + 1..].trim()));
    }
    parsed
}

pub(super) fn collect_spark_type_map_paths(
    type_name: &str,
    path: &str,
    paths: &mut std::collections::HashSet<String>,
) {
    let type_name = type_name.trim();
    let lower = type_name.to_ascii_lowercase();
    if lower.starts_with("map<") && type_name.ends_with('>') {
        paths.insert(path.to_string());
        let parts = split_top_level_spark_type_parts(&type_name[4..type_name.len() - 1]);
        if let Some(key_type) = parts.first() {
            collect_spark_type_map_paths(key_type, &format!("{path}{{key}}"), paths);
        }
        if let Some(value_type) = parts.get(1) {
            collect_spark_type_map_paths(value_type, &format!("{path}{{value}}"), paths);
        }
    } else if lower.starts_with("array<") && type_name.ends_with('>') {
        collect_spark_type_map_paths(
            &type_name[6..type_name.len() - 1],
            &format!("{path}[]"),
            paths,
        );
    } else if lower.starts_with("struct<") && type_name.ends_with('>') {
        for (field_name, child_type) in split_spark_struct_fields(type_name) {
            collect_spark_type_map_paths(child_type, &arrow_field_path(path, &field_name), paths);
        }
    }
}

pub(super) fn collect_spark_type_array_struct_paths(
    type_name: &str,
    path: &str,
    paths: &mut std::collections::HashSet<String>,
) {
    let type_name = type_name.trim();
    let lower = type_name.to_ascii_lowercase();
    if lower.starts_with("array<") && type_name.ends_with('>') {
        let element_type = type_name[6..type_name.len() - 1].trim();
        if element_type.to_ascii_lowercase().starts_with("struct<") && element_type.ends_with('>') {
            let fields = split_spark_struct_fields(element_type);
            if fields.iter().any(|(name, _)| name == "key")
                && fields.iter().any(|(name, _)| name == "value")
            {
                paths.insert(path.to_string());
            }
        }
        collect_spark_type_array_struct_paths(element_type, &format!("{path}[]"), paths);
    } else if lower.starts_with("struct<") && type_name.ends_with('>') {
        for (field_name, child_type) in split_spark_struct_fields(type_name) {
            collect_spark_type_array_struct_paths(
                child_type,
                &arrow_field_path(path, &field_name),
                paths,
            );
        }
    } else if lower.starts_with("map<") && type_name.ends_with('>') {
        let parts = split_top_level_spark_type_parts(&type_name[4..type_name.len() - 1]);
        if let Some(key_type) = parts.first() {
            collect_spark_type_array_struct_paths(key_type, &format!("{path}{{key}}"), paths);
        }
        if let Some(value_type) = parts.get(1) {
            collect_spark_type_array_struct_paths(value_type, &format!("{path}{{value}}"), paths);
        }
    }
}

pub(super) fn spark_schema_logical_map_paths(
    schema_pairs: &[(String, String)],
) -> std::collections::HashSet<String> {
    let mut paths = std::collections::HashSet::new();
    for (name, type_name) in schema_pairs {
        collect_spark_type_map_paths(type_name, &arrow_field_path("", name), &mut paths);
    }
    paths
}

pub(super) fn spark_schema_array_struct_paths(
    schema_pairs: &[(String, String)],
) -> std::collections::HashSet<String> {
    let mut paths = std::collections::HashSet::new();
    for (name, type_name) in schema_pairs {
        collect_spark_type_array_struct_paths(type_name, &arrow_field_path("", name), &mut paths);
    }
    paths
}

pub(super) fn arrow_schema_field_names(arrow_schema: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    arrow_schema
        .try_iter()?
        .map(|field| field?.getattr("name")?.extract::<String>())
        .collect()
}

pub(super) fn remap_arrow_paths_for_names(
    paths: &mut std::collections::HashSet<String>,
    arrow_schema: &Bound<'_, PyAny>,
    names: &[String],
) -> PyResult<()> {
    let source_names = arrow_schema_field_names(arrow_schema)?;
    let source_paths = paths.clone();
    let mut remapped = std::collections::HashSet::new();
    for (source, output) in source_names.iter().zip(names) {
        let source_root = arrow_field_path("", source);
        let output_root = arrow_field_path("", output);
        for path in &source_paths {
            let Some(suffix) = path.strip_prefix(&source_root) else {
                continue;
            };
            if suffix.is_empty()
                || suffix.starts_with('.')
                || suffix.starts_with('[')
                || suffix.starts_with('{')
            {
                remapped.insert(format!("{output_root}{suffix}"));
            }
        }
    }
    *paths = remapped;
    Ok(())
}

pub(super) fn rename_arrow_rows_by_position<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
    arrow_schema: &Bound<'py, PyAny>,
    names: &[String],
) -> PyResult<Bound<'py, PyAny>> {
    let source_names = arrow_schema_field_names(arrow_schema)?;
    let rows = data.cast::<PyList>()?;
    let renamed_rows = PyList::empty(py);
    for row in rows.iter() {
        let row = row.cast::<PyDict>()?;
        let renamed = PyDict::new(py);
        for (source, output) in source_names.iter().zip(names) {
            if let Some(value) = row.get_item(source.as_str())? {
                renamed.set_item(output, value)?;
            }
        }
        renamed_rows.append(renamed)?;
    }
    Ok(renamed_rows.into_any())
}

pub(super) fn arrow_array_struct_paths(
    arrow_schema: &Bound<'_, PyAny>,
) -> PyResult<std::collections::HashSet<String>> {
    let arrow_types = PyModule::import(arrow_schema.py(), "pyarrow.types")?;

    fn visit_type(
        arrow_type: &Bound<'_, PyAny>,
        path: &str,
        arrow_types: &Bound<'_, PyModule>,
        paths: &mut std::collections::HashSet<String>,
    ) -> PyResult<()> {
        let is_list = pyarrow_is_type(arrow_types, arrow_type, "is_list")?
            || pyarrow_is_type(arrow_types, arrow_type, "is_large_list")?
            || pyarrow_is_type(arrow_types, arrow_type, "is_fixed_size_list")?;
        if is_list {
            let value_type = arrow_type.getattr("value_type")?;
            if pyarrow_is_type(arrow_types, &value_type, "is_struct")? {
                let mut has_key = false;
                let mut has_value = false;
                for field in value_type.try_iter()? {
                    let field = field?;
                    let name: String = field.getattr("name")?.extract()?;
                    has_key |= name == "key";
                    has_value |= name == "value";
                }
                if has_key && has_value {
                    paths.insert(path.to_string());
                }
            }
            visit_type(&value_type, &format!("{path}[]"), arrow_types, paths)?;
        } else if pyarrow_is_type(arrow_types, arrow_type, "is_struct")? {
            for field in arrow_type.try_iter()? {
                let field = field?;
                let name: String = field.getattr("name")?.extract()?;
                let child_type = field.getattr("type")?;
                visit_type(
                    &child_type,
                    &arrow_field_path(path, &name),
                    arrow_types,
                    paths,
                )?;
            }
        } else if pyarrow_is_type(arrow_types, arrow_type, "is_map")? {
            let key_type = arrow_type.getattr("key_type")?;
            let item_type = arrow_type.getattr("item_type")?;
            visit_type(&key_type, &format!("{path}{{key}}"), arrow_types, paths)?;
            visit_type(&item_type, &format!("{path}{{value}}"), arrow_types, paths)?;
        } else if pyarrow_is_type(arrow_types, arrow_type, "is_dictionary")? {
            let value_type = arrow_type.getattr("value_type")?;
            visit_type(&value_type, path, arrow_types, paths)?;
        }
        Ok(())
    }

    let mut paths = std::collections::HashSet::new();
    for field in arrow_schema.try_iter()? {
        let field = field?;
        let arrow_type = field.getattr("type")?;
        let name: String = field.getattr("name")?.extract()?;
        visit_type(
            &arrow_type,
            &arrow_field_path("", &name),
            &arrow_types,
            &mut paths,
        )?;
    }
    Ok(paths)
}

pub(super) fn arrow_logical_map_paths(
    arrow_schema: &Bound<'_, PyAny>,
) -> PyResult<std::collections::HashSet<String>> {
    let arrow_types = PyModule::import(arrow_schema.py(), "pyarrow.types")?;

    fn visit_type(
        arrow_type: &Bound<'_, PyAny>,
        path: &str,
        arrow_types: &Bound<'_, PyModule>,
        paths: &mut std::collections::HashSet<String>,
    ) -> PyResult<()> {
        if pyarrow_is_type(arrow_types, arrow_type, "is_map")? {
            paths.insert(path.to_string());
            let key_type = arrow_type.getattr("key_type")?;
            let item_type = arrow_type.getattr("item_type")?;
            visit_type(&key_type, &format!("{path}{{key}}"), arrow_types, paths)?;
            visit_type(&item_type, &format!("{path}{{value}}"), arrow_types, paths)?;
        } else if pyarrow_is_type(arrow_types, arrow_type, "is_struct")? {
            for field in arrow_type.try_iter()? {
                let field = field?;
                let name: String = field.getattr("name")?.extract()?;
                let child_type = field.getattr("type")?;
                visit_type(
                    &child_type,
                    &arrow_field_path(path, &name),
                    arrow_types,
                    paths,
                )?;
            }
        } else if pyarrow_is_type(arrow_types, arrow_type, "is_list")?
            || pyarrow_is_type(arrow_types, arrow_type, "is_large_list")?
            || pyarrow_is_type(arrow_types, arrow_type, "is_fixed_size_list")?
        {
            let value_type = arrow_type.getattr("value_type")?;
            visit_type(&value_type, &format!("{path}[]"), arrow_types, paths)?;
        } else if pyarrow_is_type(arrow_types, arrow_type, "is_dictionary")? {
            let value_type = arrow_type.getattr("value_type")?;
            visit_type(&value_type, path, arrow_types, paths)?;
        }
        Ok(())
    }

    let mut paths = std::collections::HashSet::new();
    for field in arrow_schema.try_iter()? {
        let field = field?;
        let name: String = field.getattr("name")?.extract()?;
        let arrow_type = field.getattr("type")?;
        visit_type(
            &arrow_type,
            &arrow_field_path("", &name),
            &arrow_types,
            &mut paths,
        )?;
    }
    Ok(paths)
}

pub(super) fn arrow_types_compatible(
    py: Python<'_>,
    pa: &Bound<'_, PyModule>,
    source: &Bound<'_, PyAny>,
    current: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    let source_id = source.getattr("id")?.extract::<i32>()?;
    let current_id = current.getattr("id")?.extract::<i32>()?;
    if source_id != current_id {
        return Ok(false);
    }

    let arrow_types = pa.getattr("types")?;
    let is_type = |ty: &Bound<'_, PyAny>, predicate: &str| -> PyResult<bool> {
        arrow_types
            .getattr(predicate)?
            .call1((ty,))?
            .extract::<bool>()
    };
    if is_type(source, "is_timestamp")? {
        return source.getattr("unit")?.eq(current.getattr("unit")?);
    }
    if is_type(source, "is_list")?
        || is_type(source, "is_large_list")?
        || is_type(source, "is_fixed_size_list")?
    {
        if is_type(source, "is_fixed_size_list")?
            && !source
                .getattr("list_size")?
                .eq(current.getattr("list_size")?)?
        {
            return Ok(false);
        }
        return arrow_types_compatible(
            py,
            pa,
            &source.getattr("value_type")?,
            &current.getattr("value_type")?,
        );
    }
    if is_type(source, "is_struct")? {
        let source_count = source.getattr("num_fields")?.extract::<usize>()?;
        if source_count != current.getattr("num_fields")?.extract::<usize>()? {
            return Ok(false);
        }
        for index in 0..source_count {
            let source_field = source.call_method1("field", (index,))?;
            let current_field = current.call_method1("field", (index,))?;
            if !source_field
                .getattr("name")?
                .eq(current_field.getattr("name")?)?
                || !arrow_types_compatible(
                    py,
                    pa,
                    &source_field.getattr("type")?,
                    &current_field.getattr("type")?,
                )?
            {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if is_type(source, "is_map")? {
        return Ok(arrow_types_compatible(
            py,
            pa,
            &source.getattr("key_type")?,
            &current.getattr("key_type")?,
        )? && arrow_types_compatible(
            py,
            pa,
            &source.getattr("item_type")?,
            &current.getattr("item_type")?,
        )?);
    }

    let _ = py;
    source.eq(current)
}

pub(super) fn spark_type_to_arrow_type(
    py: Python<'_>,
    pa: &Bound<'_, PyModule>,
    data_type: &DataType,
) -> PyResult<PyObject> {
    let (name, args): (&str, Vec<PyObject>) = match data_type {
        DataType::String => ("string", Vec::new()),
        DataType::Short => ("int16", Vec::new()),
        DataType::Integer => ("int32", Vec::new()),
        DataType::Long => ("int64", Vec::new()),
        DataType::Float => ("float32", Vec::new()),
        DataType::Double => ("float64", Vec::new()),
        DataType::Decimal(precision, scale) => {
            return pa
                .getattr("decimal128")?
                .call1((*precision, *scale))?
                .into_py_any(py);
        }
        DataType::Boolean => ("bool_", Vec::new()),
        DataType::Date => ("date32", Vec::new()),
        DataType::Timestamp | DataType::TimestampNtz => {
            return pa.getattr("timestamp")?.call1(("us",))?.into_py_any(py);
        }
        DataType::Binary => ("binary", Vec::new()),
        DataType::Array(element_type) => {
            let element_type = spark_type_to_arrow_type(py, pa, element_type)?;
            return pa.getattr("list_")?.call1((element_type,))?.into_py_any(py);
        }
        DataType::Map(key_type, value_type) => {
            let key_type = spark_type_to_arrow_type(py, pa, key_type)?;
            let value_type = spark_type_to_arrow_type(py, pa, value_type)?;
            return pa
                .getattr("map_")?
                .call1((key_type, value_type))?
                .into_py_any(py);
        }
        DataType::LogicalStruct(fields) => {
            let py_fields = PyList::empty(py);
            for field in fields {
                let field_type = spark_type_to_arrow_type(py, pa, &field.data_type)?;
                py_fields.append((field.name.as_str(), field_type))?;
            }
            return pa.getattr("struct")?.call1((py_fields,))?.into_py_any(py);
        }
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unsupported Spark data type {data_type:?}"
            )));
        }
    };
    let args = PyTuple::new(py, args)?;
    pa.getattr(name)?.call1(args)?.into_py_any(py)
}

pub(super) fn spark_type_to_arrow_type_with_maps(
    py: Python<'_>,
    pa: &Bound<'_, PyModule>,
    data_type: &DataType,
    path: &str,
    map_paths: &std::collections::HashSet<String>,
) -> PyResult<PyObject> {
    if map_paths.contains(path) {
        let (key_type, value_type) = match data_type {
            DataType::Map(key_type, value_type) => (key_type.as_ref(), value_type.as_ref()),
            DataType::Array(element_type) => {
                let DataType::LogicalStruct(fields) = element_type.as_ref() else {
                    return spark_type_to_arrow_type(py, pa, data_type);
                };
                let (Some(key_type), Some(value_type)) = (
                    fields.iter().find(|field| field.name == "key"),
                    fields.iter().find(|field| field.name == "value"),
                ) else {
                    return spark_type_to_arrow_type(py, pa, data_type);
                };
                (&key_type.data_type, &value_type.data_type)
            }
            _ => return spark_type_to_arrow_type(py, pa, data_type),
        };
        let key_type = spark_type_to_arrow_type_with_maps(
            py,
            pa,
            key_type,
            &format!("{path}{{key}}"),
            map_paths,
        )?;
        let value_type = spark_type_to_arrow_type_with_maps(
            py,
            pa,
            value_type,
            &format!("{path}{{value}}"),
            map_paths,
        )?;
        return pa
            .getattr("map_")?
            .call1((key_type, value_type))?
            .into_py_any(py);
    }

    match data_type {
        DataType::Array(element_type) => {
            let element_type = spark_type_to_arrow_type_with_maps(
                py,
                pa,
                element_type,
                &format!("{path}[]"),
                map_paths,
            )?;
            pa.getattr("list_")?.call1((element_type,))?.into_py_any(py)
        }
        DataType::LogicalStruct(fields) => {
            let py_fields = PyList::empty(py);
            for field in fields {
                let field_type = spark_type_to_arrow_type_with_maps(
                    py,
                    pa,
                    &field.data_type,
                    &arrow_field_path(path, &field.name),
                    map_paths,
                )?;
                py_fields.append((field.name.as_str(), field_type))?;
            }
            pa.getattr("struct")?.call1((py_fields,))?.into_py_any(py)
        }
        DataType::Map(key_type, value_type) => {
            let key_type = spark_type_to_arrow_type_with_maps(
                py,
                pa,
                key_type,
                &format!("{path}{{key}}"),
                map_paths,
            )?;
            let value_type = spark_type_to_arrow_type_with_maps(
                py,
                pa,
                value_type,
                &format!("{path}{{value}}"),
                map_paths,
            )?;
            pa.getattr("map_")?
                .call1((key_type, value_type))?
                .into_py_any(py)
        }
        _ => spark_type_to_arrow_type(py, pa, data_type),
    }
}

pub(super) fn normalize_arrow_value_with_maps(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    data_type: &DataType,
    path: &str,
    map_paths: &std::collections::HashSet<String>,
) -> PyResult<PyObject> {
    if value.is_none() {
        return value.clone().unbind().into_py_any(py);
    }

    if map_paths.contains(path) {
        let (key_type, value_type) = match data_type {
            DataType::Map(key_type, value_type) => (key_type.as_ref(), value_type.as_ref()),
            DataType::Array(element_type) => {
                let DataType::LogicalStruct(fields) = element_type.as_ref() else {
                    return value.clone().unbind().into_py_any(py);
                };
                let (Some(key), Some(item)) = (
                    fields.iter().find(|field| field.name == "key"),
                    fields.iter().find(|field| field.name == "value"),
                ) else {
                    return value.clone().unbind().into_py_any(py);
                };
                (&key.data_type, &item.data_type)
            }
            _ => return value.clone().unbind().into_py_any(py),
        };
        let items = PyList::empty(py);
        if let Ok(dict) = value.cast::<PyDict>() {
            for (key, item) in dict.iter() {
                let key = normalize_arrow_value_with_maps(
                    py,
                    &key,
                    key_type,
                    &format!("{path}{{key}}"),
                    map_paths,
                )?;
                let item = normalize_arrow_value_with_maps(
                    py,
                    &item,
                    value_type,
                    &format!("{path}{{value}}"),
                    map_paths,
                )?;
                items.append(PyTuple::new(py, [key, item])?)?;
            }
        } else {
            for entry in value.try_iter()? {
                let entry = entry?;
                let entry_dict = if entry.is_instance_of::<PyDict>() {
                    entry.clone()
                } else if entry.hasattr("asDict")? {
                    entry.call_method0("asDict")?
                } else {
                    entry.clone()
                };
                let (key, item) = if entry_dict.is_instance_of::<PyDict>() {
                    (entry_dict.get_item("key")?, entry_dict.get_item("value")?)
                } else {
                    (entry.get_item(0)?, entry.get_item(1)?)
                };
                let key = normalize_arrow_value_with_maps(
                    py,
                    &key,
                    key_type,
                    &format!("{path}{{key}}"),
                    map_paths,
                )?;
                let item = normalize_arrow_value_with_maps(
                    py,
                    &item,
                    value_type,
                    &format!("{path}{{value}}"),
                    map_paths,
                )?;
                items.append(PyTuple::new(py, [key, item])?)?;
            }
        }
        return items.into_py_any(py);
    }

    match data_type {
        DataType::LogicalStruct(fields) => {
            let source = if value.is_instance_of::<PyDict>() {
                value.clone()
            } else if value.hasattr("asDict")? {
                value.call_method0("asDict")?
            } else {
                return value.clone().unbind().into_py_any(py);
            };
            let output = PyDict::new(py);
            for field in fields {
                let child = source.get_item(field.name.as_str())?;
                let child = normalize_arrow_value_with_maps(
                    py,
                    &child,
                    &field.data_type,
                    &arrow_field_path(path, &field.name),
                    map_paths,
                )?;
                output.set_item(field.name.as_str(), child.bind(py))?;
            }
            output.into_py_any(py)
        }
        DataType::Array(element_type) => {
            let output = PyList::empty(py);
            for item in value.try_iter()? {
                let item = item?;
                let item = normalize_arrow_value_with_maps(
                    py,
                    &item,
                    element_type,
                    &format!("{path}[]"),
                    map_paths,
                )?;
                output.append(item.bind(py))?;
            }
            output.into_py_any(py)
        }
        DataType::Map(key_type, value_type) => {
            let mut nested_map_paths = map_paths.clone();
            nested_map_paths.insert(path.to_string());
            normalize_arrow_value_with_maps(
                py,
                value,
                &DataType::Map(key_type.clone(), value_type.clone()),
                path,
                &nested_map_paths,
            )
        }
        _ => value.clone().unbind().into_py_any(py),
    }
}
