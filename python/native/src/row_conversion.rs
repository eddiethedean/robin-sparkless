//! Python row normalization, schema parsing, and type inference.

use super::*;

/// Map PySpark type name to our schema type string.
/// Pass through array<...>, map<...>, struct<...> so createDataFrame preserves List/Map/Struct columns.
pub(super) fn simple_string_to_type(s: &str) -> String {
    let trimmed = s.trim();
    let lower = trimmed.to_lowercase();
    // Pass through decimal(p,s) so explicit DDL schema is not re-inferred as all-string (#1544).
    if lower.starts_with("decimal(") && lower.contains(')') {
        return trimmed.to_string();
    }
    match lower.as_str() {
        "string" | "str" => "string".to_string(),
        "int" | "integer" => "int".to_string(),
        "short" | "smallint" => "smallint".to_string(),
        "int32" => "int32".to_string(),
        "bigint" | "long" | "int64" => "long".to_string(),
        "double" | "float64" => "double".to_string(),
        "float" | "float32" => "float".to_string(),
        "decimal" => "decimal".to_string(),
        "boolean" | "bool" => "boolean".to_string(),
        "date" => "date".to_string(),
        "timestamp" => "timestamp".to_string(),
        "timestamp_ntz" => "timestamp_ntz".to_string(),
        "binary" => "binary".to_string(),
        "list" | "array" => lower,
        _ if lower.starts_with("array<") && lower.ends_with('>') => trimmed.to_string(),
        _ if lower.starts_with("map<") && lower.ends_with('>') => trimmed.to_string(),
        _ if lower.starts_with("struct<") && lower.ends_with('>') => trimmed.to_string(),
        _ => "string".to_string(),
    }
}

/// Explicit schema string types use "str" (not "string") so Rust does not re-infer from data (#1603).
pub(super) fn mark_explicit_schema_string_type(typ: String) -> String {
    if typ.eq_ignore_ascii_case("string") {
        "str".to_string()
    } else {
        typ
    }
}

/// Parse DDL schema string (e.g. "name: string, age: int" or "a string, b int") into (name, type) pairs.
pub(super) fn parse_ddl_schema_string(ddl: &str) -> Vec<(String, String)> {
    fn split_top_level_commas(s: &str) -> Vec<String> {
        let mut parts: Vec<String> = Vec::new();
        let mut buf = String::new();
        let mut depth: i32 = 0; // struct<...>, decimal(p,s), array<...>
        for ch in s.chars() {
            match ch {
                '(' | '<' => {
                    depth += 1;
                    buf.push(ch);
                }
                ')' | '>' => {
                    depth = (depth - 1).max(0);
                    buf.push(ch);
                }
                ',' if depth == 0 => {
                    let part = buf.trim();
                    if !part.is_empty() {
                        parts.push(part.to_string());
                    }
                    buf.clear();
                }
                _ => buf.push(ch),
            }
        }
        let part = buf.trim();
        if !part.is_empty() {
            parts.push(part.to_string());
        }
        parts
    }

    fn find_top_level_char(s: &str, target: char) -> Option<usize> {
        let mut depth: i32 = 0;
        for (i, ch) in s.char_indices() {
            match ch {
                '(' | '<' => depth += 1,
                ')' | '>' => depth = (depth - 1).max(0),
                _ => {}
            }
            if depth == 0 && ch == target {
                return Some(i);
            }
        }
        None
    }

    fn rfind_top_level_space(s: &str) -> Option<usize> {
        let mut depth: i32 = 0;
        for (i, ch) in s.char_indices().rev() {
            match ch {
                ')' | '>' => depth += 1,
                '(' | '<' => depth = (depth - 1).max(0),
                _ => {}
            }
            if depth == 0 && ch == ' ' {
                return Some(i);
            }
        }
        None
    }

    let mut pairs = Vec::new();
    for part in split_top_level_commas(ddl) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, typ) = if let Some(colon) = find_top_level_char(part, ':') {
            let name = part[..colon].trim().to_string();
            let typ = part[colon + 1..].trim().to_string();
            (name, typ)
        } else {
            // "name type" format: last space separates name and type
            match rfind_top_level_space(part) {
                Some(i) => (
                    part[..i].trim().to_string(),
                    part[i + 1..].trim().to_string(),
                ),
                None => (part.to_string(), "string".to_string()),
            }
        };
        if !name.is_empty() {
            pairs.push((
                name,
                mark_explicit_schema_string_type(simple_string_to_type(&typ)),
            ));
        }
    }
    pairs
}

/// Parse schema from Python: None, list of column names (str), list of (name, type) pairs,
/// DDL string (e.g. "name: string, age: int"), or StructType-like object with .fields.
pub(super) fn parse_schema_from_py(
    _py: Python<'_>,
    schema: &Bound<'_, PyAny>,
) -> PyResult<Option<Vec<(String, String)>>> {
    // StructType-like: has .fields
    if let Ok(fields) = schema.getattr("fields") {
        let list = fields.cast::<PyList>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("schema.fields must be a list")
        })?;
        let mut pairs = Vec::with_capacity(list.len());
        for item in list.iter() {
            let name: String = item.getattr("name")?.extract()?;
            let data_type = item.getattr("dataType")?;
            let typ: String = data_type
                .call_method0("simpleString")
                .or_else(|_| data_type.getattr("simpleString").and_then(|s| s.call0()))
                .and_then(|v| v.extract::<String>())
                .unwrap_or_else(|_| "string".to_string());
            let mapped = simple_string_to_type(&typ);
            // Explicit StructType schema should not be re-inferred from data even if all fields are strings.
            let mapped = if mapped.eq_ignore_ascii_case("string") {
                "str".to_string()
            } else {
                mapped
            };
            pairs.push((name, mapped));
        }
        return Ok(Some(pairs));
    }

    // DDL string: "name: string, age: int" or "a string, b int"
    // OR single-column dtype string: "bigint", "int", "string", etc. (issue #419 parity).
    if let Ok(ddl) = schema.extract::<String>() {
        let ddl = ddl.trim();
        if !ddl.contains(',') && !ddl.contains(':') && !ddl.contains(' ') {
            return Ok(Some(vec![(
                "value".to_string(),
                simple_string_to_type(ddl),
            )]));
        }
        let pairs = parse_ddl_schema_string(ddl);
        return Ok(Some(pairs));
    }

    // DataType-like object (e.g. IntegerType()): has .simpleString()
    if let Ok(ss) = schema
        .call_method0("simpleString")
        .and_then(|v| v.extract::<String>())
    {
        return Ok(Some(vec![(
            "value".to_string(),
            simple_string_to_type(&ss),
        )]));
    }

    let list = schema.cast::<PyList>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "schema must be None, str (DDL or dtype), list, or StructType-like",
        )
    })?;
    if list.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let first = list.get_item(0)?;
    if first.cast::<PyList>().is_ok() || first.cast::<pyo3::types::PyTuple>().is_ok() {
        let mut pairs = Vec::with_capacity(list.len());
        for item in list.iter() {
            let pair = item.cast::<pyo3::types::PyTuple>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "schema must be list of column names or list of (name, type)",
                )
            })?;
            if pair.len() != 2 {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "schema pair must be (name, type)",
                ));
            }
            let name = pair.get_item(0)?.extract::<String>()?;
            let typ = pair.get_item(1)?.extract::<String>()?;
            pairs.push((
                name,
                mark_explicit_schema_string_type(simple_string_to_type(&typ)),
            ));
        }
        return Ok(Some(pairs));
    }
    let mut names = Vec::with_capacity(list.len());
    for item in list.iter() {
        let v = item.extract::<String>();
        if let Ok(s) = v {
            names.push(s);
        } else {
            // might be a type object (e.g. StringType()) - use "string"
            names.push("string".to_string());
        }
    }
    Ok(Some(
        names
            .into_iter()
            .map(|n| (n, "string".to_string()))
            .collect(),
    ))
}

pub(super) fn infer_type_from_json_value(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "string".to_string(),
        JsonValue::String(s) if s.starts_with("__sparkless_decimal__:__sparkless_decimal__:") => {
            "string".to_string()
        }
        JsonValue::String(s) if s.starts_with("__sparkless_decimal__:") => {
            let decimal = s.strip_prefix("__sparkless_decimal__:").unwrap_or(s);
            let (mantissa, exponent) = decimal
                .split_once(['e', 'E'])
                .and_then(|(mantissa, exponent)| {
                    exponent.parse::<i32>().ok().map(|e| (mantissa, e))
                })
                .unwrap_or((decimal, 0));
            let unsigned_mantissa = mantissa.trim_start_matches(['-', '+']);
            let (whole, fraction) = unsigned_mantissa
                .split_once('.')
                .unwrap_or((unsigned_mantissa, ""));
            let scale = (fraction.len() as i32 - exponent).max(0) as usize;
            let significant_digits = format!("{whole}{fraction}").trim_start_matches('0').len();
            let precision = (significant_digits as i32 + (exponent - fraction.len() as i32).max(0))
                .max(scale as i32)
                .max(1) as usize;
            format!("decimal({precision},{scale})")
        }
        JsonValue::Bool(_) => "boolean".to_string(),
        JsonValue::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "long".to_string()
            } else {
                "double".to_string()
            }
        }
        JsonValue::String(_) => "string".to_string(),
        // Treat JSON arrays as array type so list columns (e.g. scores, tags) are inferred
        // as List/Array instead of String when creating DataFrames from Python data.
        JsonValue::Array(arr) => {
            if arr.is_empty() {
                "array".to_string()
            } else {
                let elem = merge_inferred_types(arr.iter().map(infer_type_from_json_value));
                format_array_type_name(&elem)
            }
        }
        JsonValue::Object(_) => "string".to_string(),
    }
}

/// Normalize createDataFrame input: convert pandas, PyArrow Table, NumPy ndarray, or RDD to list.
/// Returns (converted list or original data, from_pandas flag, pandas column order if from pandas).
pub(super) fn normalize_create_dataframe_input<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
    schema: Option<&Bound<'py, PyAny>>,
) -> PyResult<(Bound<'py, PyAny>, bool, Option<Vec<String>>)> {
    // 0. RDD (df.rdd): collect to list of dicts for createDataFrame(rdd, schema) (#1147 / #361).
    // When schema is provided, pass its column names so dict keys match (e.g. "Name", "Value").
    if let Ok(rdd) = data.cast::<PyRDD>() {
        let preferred_names: Option<Vec<String>> = schema
            .and_then(|s| parse_schema_from_py(py, s).ok().flatten())
            .map(|pairs| pairs.into_iter().map(|(n, _)| n).collect());
        let list = if let Some(names) = preferred_names {
            let py_names = PyList::empty(py);
            for n in &names {
                py_names.append(n)?;
            }
            rdd.call_method1("_to_pylist", (py_names,))?
        } else {
            rdd.call_method0("_to_pylist")?
        };
        // Return list directly so downstream uses our dicts; no need to re-normalize.
        return Ok((list, false, None));
    }

    // 1. PyArrow Table: to_pylist() returns list of dicts
    if let Ok(to_pylist) = data.getattr("to_pylist") {
        if to_pylist.is_callable() {
            let list = to_pylist.call0()?;
            if let Ok(rows) = list.cast::<PyList>() {
                // PyArrow represents map entries as lists of pairs at every nesting
                // level; Spark's MapType input contract is a Python dict.
                if is_pyarrow_table_or_batch(data) {
                    let arrow_schema = data.getattr("schema")?;
                    let arrow_types = PyModule::import(py, "pyarrow.types")?;
                    for field in arrow_schema.try_iter()? {
                        let field = field?;
                        let name: String = field.getattr("name")?.extract()?;
                        let field_type = field.getattr("type")?;
                        for row in rows.iter() {
                            let row = row.cast::<PyDict>()?;
                            let Some(value) = row.get_item(name.as_str())? else {
                                continue;
                            };
                            if value.is_none() {
                                continue;
                            }
                            let normalized =
                                normalize_arrow_value(py, &value, &field_type, &arrow_types)?;
                            row.set_item(name.as_str(), normalized.bind(py))?;
                        }
                    }
                }
                return Ok((list, true, None));
            }
        }
    }

    // 2. NumPy ndarray: tolist() + handle 1D (wrap each element as single-column row)
    if let Ok(ndim_attr) = data.getattr("ndim") {
        if let Ok(ndim) = ndim_attr.extract::<i32>() {
            if let Ok(tolist) = data.getattr("tolist") {
                if tolist.is_callable() {
                    let list = tolist.call0()?;
                    if ndim == 1 {
                        let py_list = list.cast::<PyList>().map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                "numpy 1D array tolist() must return a list",
                            )
                        })?;
                        let out = PyList::empty(py);
                        for item in py_list.iter() {
                            let row = PyList::empty(py);
                            row.append(item)?;
                            out.append(row)?;
                        }
                        return Ok((out.into_any(), false, None));
                    }
                    if ndim == 2 && list.cast::<PyList>().is_ok() {
                        return Ok((list, false, None));
                    }
                }
            }
        }
    }

    // 3. Pandas DataFrame: to_dict("records"); preserve column order (#1151).
    let (data, from_pandas, pandas_columns) = maybe_convert_pandas_to_list(py, data)?;
    let data = coerce_create_dataframe_rows_to_list(py, &data)?;
    Ok((data, from_pandas, pandas_columns))
}

/// Convert tuple or other row iterable to `list` for createDataFrame (PySpark parity; #1542).
pub(super) fn coerce_create_dataframe_rows_to_list<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if data.cast::<PyList>().is_ok() {
        return Ok(data.clone());
    }
    if let Ok(tup) = data.cast::<PyTuple>() {
        let out = PyList::empty(py);
        for item in tup.iter() {
            out.append(item)?;
        }
        return Ok(out.into_any());
    }
    // Generators and other iterables of rows; exclude str/bytes (iterable of chars).
    if !data.is_instance_of::<PyString>() && !data.is_instance_of::<PyBytes>() {
        if let Ok(iter) = data.try_iter() {
            let out = PyList::empty(py);
            for item in iter {
                out.append(item?)?;
            }
            return Ok(out.into_any());
        }
    }
    Ok(data.clone())
}

/// If data is a pandas DataFrame, convert to list of dicts via to_dict("records").
/// Returns (converted list, true, Some(column_names_in_order)) when from pandas (#1151).
pub(super) fn maybe_convert_pandas_to_list<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
) -> PyResult<(Bound<'py, PyAny>, bool, Option<Vec<String>>)> {
    let to_dict = match data.getattr("to_dict") {
        Ok(attr) => attr,
        Err(_) => return Ok((data.clone(), false, None)),
    };
    if !to_dict.is_callable() {
        return Ok((data.clone(), false, None));
    }
    // Preserve column order from pandas DataFrame (PySpark parity; #1151).
    let pandas_columns: Option<Vec<String>> = data
        .getattr("columns")
        .ok()
        .and_then(|cols| cols.call_method0("tolist").ok())
        .and_then(|list| list.extract::<Vec<String>>().ok());
    let records = match to_dict.call1(("records",)) {
        Ok(r) => r,
        Err(_) => return Ok((data.clone(), false, None)),
    };
    if records.cast::<PyList>().is_ok() {
        // pandas uses NaN for missing values in object (string) columns.
        // Replace NaN values with None so they become null in the DataFrame.
        // This fixes the issue where str(float('nan')) == 'nan' instead of None.
        let cleaned = replace_nan_with_none_in_records(py, &records)?;
        Ok((cleaned, true, pandas_columns))
    } else {
        Ok((data.clone(), false, None))
    }
}

/// Replace float NaN values with None in a list of dicts (pandas records).
/// This is needed because pandas uses NaN for missing values in object columns.
pub(super) fn replace_nan_with_none_in_records<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let list = records.cast::<PyList>()?;
    let result = PyList::empty(py);

    for record in list.iter() {
        if let Ok(dict) = record.cast::<PyDict>() {
            let new_dict = PyDict::new(py);
            for (k, v) in dict.iter() {
                // Check if value is a float NaN
                let is_nan = if let Ok(f) = v.extract::<f64>() {
                    f.is_nan()
                } else {
                    false
                };
                if is_nan {
                    new_dict.set_item(k, py.None())?;
                } else {
                    new_dict.set_item(k, v)?;
                }
            }
            result.append(new_dict)?;
        } else {
            result.append(record)?;
        }
    }
    Ok(result.into_any())
}

#[allow(clippy::type_complexity)]
pub(super) fn python_data_and_schema(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    schema: Option<&Bound<'_, PyAny>>,
    from_pandas: bool,
    pandas_column_order: Option<&[String]>,
) -> PyResult<(Vec<Vec<JsonValue>>, Vec<(String, String)>, bool, bool)> {
    // #1346: PySpark raises PySparkTypeError [CANNOT_ACCEPT_OBJECT_IN_TYPE]; match type and message.
    let type_name: String = data
        .get_type()
        .name()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let list = data.cast::<PyList>().map_err(|_| {
        SparklessError::new_err(format!(
            "[CANNOT_ACCEPT_OBJECT_IN_TYPE] `StructType` can not accept object in type `{type_name}`."
        ))
    })?;
    // PySpark parity: createDataFrame([]) without explicit schema raises (test expects match CANNOT_INFER_EMPTY_SCHEMA).
    if list.is_empty() && schema.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema from empty dataset.",
        ));
    }
    // Parse explicit schema first so we can use its field order when building rows from dicts (Phase 7 / issue_247).
    let schema_names_only: bool = schema
        .and_then(|s| s.cast::<PyList>().ok())
        .map(|l| l.iter().all(|it| it.extract::<String>().is_ok()))
        .unwrap_or(false);
    // PySpark parity: createDataFrame([], [column names]) also raises (test_issue_372).
    if list.is_empty() && schema_names_only {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema from empty dataset.",
        ));
    }
    let schema_res: Option<Vec<(String, String)>> = schema
        .map(|s| parse_schema_from_py(py, s))
        .transpose()?
        .and_then(|o| o);
    // Scalar rows (e.g. [1,2,3]) are only allowed when there is an explicit single-column schema
    // such as "bigint" or DateType() (PySpark parity for single-type createDataFrame).
    let allow_scalar_single_column: bool =
        schema_res.as_ref().map(|s| s.len() == 1).unwrap_or(false);
    // Column order for dict rows: pandas order (#1151), else schema field order when the user
    // supplied a schema (including names-only lists), else union-of-keys alphabetical.
    let mut column_order: Option<Vec<String>> =
        pandas_column_order.map(|s| s.to_vec()).or_else(|| {
            schema_res
                .as_ref()
                .map(|s| s.iter().map(|(n, _)| n.clone()).collect())
        });
    if column_order.is_none() && !list.is_empty() {
        use std::collections::BTreeSet;
        let mut keys_union: BTreeSet<String> = BTreeSet::new();
        for item in list.iter() {
            if let Ok(dict) = item.cast::<PyDict>() {
                for (k, _) in dict.iter() {
                    if let Ok(name) = k.extract::<String>() {
                        keys_union.insert(name);
                    }
                }
            }
        }
        if !keys_union.is_empty() {
            column_order = Some(keys_union.into_iter().collect());
        }
    }

    let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(list.len());
    let mut inferred_schema: Option<Vec<(String, String)>> = None;
    let mut row_kind: Option<&'static str> = None;
    let mut mixed_row_kinds: bool = false;

    // #1267: when we have column order, build rows in Python so dict.get(k) runs in Python
    // and keys are found correctly (avoids PyO3 dict lookup issues).
    if !list.is_empty() {
        if let Some(keys) = column_order.as_ref() {
            let keys_py = PyList::empty(py);
            for k in keys {
                keys_py.append(k.as_str())?;
            }
            // Tuple/list rows cannot be dict()-normalized; batch dict helpers only apply to dict rows (#1544).
            let all_dict_rows = list.iter().all(|item| item.cast::<PyDict>().is_ok());
            // Normalize to list of plain dicts so d.get(k) finds keys regardless of key type (#357, #1267).
            let data_for_batch: Bound<'_, PyList> = if all_dict_rows {
                (|| {
                    let out = PyList::empty(py);
                    let builtins = py.import("builtins").ok()?;
                    let dict_fn = builtins.getattr("dict").ok()?;
                    for item in list.iter() {
                        let normalized = dict_fn.call1((item,)).ok()?;
                        out.append(normalized).ok()?;
                    }
                    Some(out)
                })()
                .unwrap_or_else(|| list.clone())
            } else {
                list.clone()
            };
            let batch_dict_rows = all_dict_rows;
            // Prefer dict_rows_to_column_order from Python (load _cdf_helpers then get from sys.modules); then eval.
            let try_helper = || -> PyResult<Option<Bound<'_, PyList>>> {
                let _ = py.import("sparkless._cdf_helpers").ok(); // ensure loaded
                let sys = py.import("sys")?;
                let modules = sys.getattr("modules")?;
                let mod_ = modules.get_item("sparkless._cdf_helpers").map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyImportError, _>(
                        "sparkless._cdf_helpers not in sys.modules",
                    )
                })?;
                let func = mod_.getattr("dict_rows_to_column_order")?;
                let result = func.call1((data_for_batch.clone(), keys_py.clone()))?;
                let list: Bound<'_, PyList> = result.cast::<PyList>()?.clone();
                Ok(Some(list))
            };
            let rows_py = try_helper().ok().flatten().or_else(|| {
                let globals = PyDict::new(py);
                if globals.set_item("_cdf_d", &data_for_batch).is_err() {
                    return None;
                }
                if globals.set_item("_cdf_k", &keys_py).is_err() {
                    return None;
                }
                let builtins = py.import("builtins").ok()?;
                if globals.set_item("__builtins__", &builtins).is_err() {
                    return None;
                }
                let code = "[[d.get(k) for k in _cdf_k] for d in _cdf_d]";
                let result = builtins
                    .getattr("eval")
                    .ok()?
                    .call1((code, &globals))
                    .ok()?;
                result.cast::<PyList>().ok().cloned()
            });
            if batch_dict_rows {
                if let Some(rows_py) = rows_py {
                    if rows_py.len() == list.len() {
                        let mut parsed = Vec::with_capacity(rows_py.len());
                        for row_py in rows_py.iter() {
                            if let Ok(row_list) = row_py.cast::<PyList>() {
                                let mut row = Vec::with_capacity(row_list.len());
                                for v in row_list.iter() {
                                    row.push(py_any_to_json(py, &v)?);
                                }
                                parsed.push(row);
                            } else {
                                break;
                            }
                        }
                        // Only use batch result if first column is present (avoid wrong lookup from eval/helper).
                        if parsed.len() == list.len()
                            && parsed
                                .first()
                                .and_then(|r| r.first())
                                .map(|v| !matches!(v, JsonValue::Null))
                                .unwrap_or(false)
                        {
                            rows.extend(parsed);
                            row_kind = Some("dict");
                        }
                    }
                }
            }
        }
    }
    // Per-row lambda for dict-like rows when batch lambda failed: [row.get(k) for k in keys].
    let per_row_fn = if let Some(keys) = &column_order {
        let kpy = PyList::empty(py);
        for k in keys {
            let _ = kpy.append(k.as_str());
        }
        let g = PyDict::new(py);
        if let Ok(builtins) = py.import("builtins") {
            let _ = g.set_item("__builtins__", &builtins);
        }
        let code = "(lambda row, keys: [row.get(k) for k in keys])";
        let f = py
            .import("builtins")
            .ok()
            .and_then(|b| b.getattr("eval").ok())
            .and_then(|ev| ev.call1((code, &g)).ok());
        f.map(|func| (func, kpy))
    } else {
        None
    };
    if row_kind.is_none() {
        for (idx, item) in list.iter().enumerate() {
            let kind = if item.cast::<PyDict>().is_ok() {
                "dict"
            } else if item.cast::<PyList>().is_ok() || item.cast::<pyo3::types::PyTuple>().is_ok() {
                "seq"
            } else {
                "scalar"
            };
            if let Some(first) = row_kind {
                if first != kind {
                    mixed_row_kinds = true;
                }
            } else {
                row_kind = Some(kind);
            }
            // For native dicts with column_order: get each value in Python via row.get(k) (#1267).
            let row = if item.cast::<PyDict>().is_ok() {
                if let Some(order) = column_order.as_ref() {
                    let from_eval: Option<Vec<JsonValue>> = (|| {
                        let builtins = py.import("builtins").ok()?;
                        let eval_fn = builtins.getattr("eval").ok()?;
                        let globals = PyDict::new(py);
                        globals.set_item("_row", &item).ok()?;
                        globals.set_item("__builtins__", &builtins).ok()?;
                        let mut values = Vec::with_capacity(order.len());
                        for k in order {
                            let py_k = pyo3::types::PyString::new(py, k.as_str());
                            globals.set_item("_k", &py_k).ok()?;
                            let result = eval_fn.call1(("_row.get(_k)", &globals)).ok()?;
                            let v = py_any_to_json(py, &result).ok().unwrap_or(JsonValue::Null);
                            values.push(v);
                        }
                        Some(values)
                    })();
                    if let Some(values) = from_eval {
                        values
                    } else {
                        python_row_to_json(
                            py,
                            &item,
                            idx,
                            column_order.as_deref(),
                            from_pandas,
                            allow_scalar_single_column,
                            schema_names_only,
                        )?
                    }
                } else {
                    python_row_to_json(
                        py,
                        &item,
                        idx,
                        column_order.as_deref(),
                        from_pandas,
                        allow_scalar_single_column,
                        schema_names_only,
                    )?
                }
            } else if item.cast::<PyDict>().is_ok() {
                python_row_to_json(
                    py,
                    &item,
                    idx,
                    column_order.as_deref(),
                    from_pandas,
                    allow_scalar_single_column,
                    schema_names_only,
                )?
            } else if let Some((ref row_fn, ref kpy)) = per_row_fn {
                if item
                    .getattr("get")
                    .map(|g| g.is_callable())
                    .unwrap_or(false)
                {
                    if let Ok(row_py) = row_fn.call1((&item, kpy)) {
                        if let Ok(row_list) = row_py.cast::<PyList>() {
                            let mut r = Vec::with_capacity(row_list.len());
                            for v in row_list.iter() {
                                if let Ok(j) = py_any_to_json(py, &v) {
                                    r.push(j);
                                } else {
                                    r.push(JsonValue::Null);
                                }
                            }
                            r
                        } else {
                            python_row_to_json(
                                py,
                                &item,
                                idx,
                                column_order.as_deref(),
                                from_pandas,
                                allow_scalar_single_column,
                                schema_names_only,
                            )?
                        }
                    } else {
                        python_row_to_json(
                            py,
                            &item,
                            idx,
                            column_order.as_deref(),
                            from_pandas,
                            allow_scalar_single_column,
                            schema_names_only,
                        )?
                    }
                } else {
                    python_row_to_json(
                        py,
                        &item,
                        idx,
                        column_order.as_deref(),
                        from_pandas,
                        allow_scalar_single_column,
                        schema_names_only,
                    )?
                }
            } else {
                python_row_to_json(
                    py,
                    &item,
                    idx,
                    column_order.as_deref(),
                    from_pandas,
                    allow_scalar_single_column,
                    schema_names_only,
                )?
            };
            // For dict rows without explicit schema, infer full schema from all rows/keys later so we
            // can include sparse keys (issue #372). For other row kinds, infer from the first row.
            if inferred_schema.is_none()
                && schema_res.is_none()
                && kind != "dict"
                && !row.is_empty()
            {
                if let Some(cols) = infer_schema_from_first_row(py, &item, from_pandas) {
                    inferred_schema = Some(cols);
                }
            }
            rows.push(row);
        }
    }

    // Names-only list schema should still infer types from data (PySpark parity).
    let schema_was_inferred = schema_res.is_none() || schema_names_only;
    // When schema is not explicit and rows are dicts, build schema from the union of keys so
    // sparse rows (different keys) still include all columns, ordered alphabetically, and infer
    // types from the underlying Python values (so bytes -> BinaryType, etc.).
    let mut schema = if schema_res.is_none() && matches!(row_kind, Some("dict")) {
        let names: Vec<String> = if let Some(order) = &column_order {
            order.clone()
        } else {
            rows.first()
                .map(|r| {
                    (0..r.len())
                        .map(|i| format!("_{}", i + 1))
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default()
        };
        let mut out: Vec<(String, String)> = Vec::with_capacity(names.len());
        let cfg = robin_sparkless::get_thread_runtime_config();
        for name in &names {
            let mut dtype = "string".to_string();
            for item in list.iter() {
                if let Ok(dict) = item.cast::<PyDict>() {
                    if let Ok(Some(v)) = dict.get_item(name) {
                        if cfg.infer_map_type_from_first_pair {
                            dtype = infer_type_from_py_value(&v);
                            break;
                        } else {
                            dtype = merge_spark_type_names(&dtype, &infer_type_from_py_value(&v));
                        }
                    }
                }
            }
            out.push((name.clone(), dtype));
        }
        out
    } else {
        schema_res.or(inferred_schema).unwrap_or_else(|| {
            rows.first()
                .map(|r| {
                    (0..r.len())
                        .map(|i| (format!("_{}", i + 1), "string".to_string()))
                        .collect()
                })
                .unwrap_or_default()
        })
    };
    // #1642: names-only schema with tuple/list rows — PySpark adds _4, _5, ... for extra values.
    if schema_names_only {
        if let Some(max_len) = rows.iter().map(|r| r.len()).max() {
            while schema.len() < max_len {
                schema.push((format!("_{}", schema.len() + 1), "string".to_string()));
            }
            for row in &mut rows {
                while row.len() < schema.len() {
                    row.push(JsonValue::Null);
                }
            }
        }
    }
    if let (Some(first_row), true) = (rows.first(), schema.iter().all(|(_, t)| t == "string")) {
        if first_row.len() == schema.len() {
            schema = schema
                .into_iter()
                .zip(first_row.iter())
                .map(|((name, _), v)| (name, infer_type_from_json_value(v)))
                .collect();
        }
    }
    // When schema was inferred, prefer first non-null value's type per column (fixes fillna(0) on column with null in first row).
    if schema_was_inferred && !schema.is_empty() {
        let refined: Vec<(String, String)> = schema
            .iter()
            .enumerate()
            .map(|(i, (name, typ))| {
                let better = rows
                    .iter()
                    .filter_map(|r| r.get(i))
                    .filter(|v| !matches!(v, JsonValue::Null))
                    .map(infer_type_from_json_value)
                    .reduce(|acc, t| merge_spark_type_names(&acc, &t));
                // Refine string-like and untyped array/list columns by merging types across all rows.
                let is_untyped_container =
                    typ.eq_ignore_ascii_case("array") || typ.eq_ignore_ascii_case("list");
                let is_decimal = typ.starts_with("decimal(");
                let final_typ = if is_decimal {
                    better
                        .filter(|inferred| inferred.starts_with("decimal("))
                        .unwrap_or_else(|| typ.clone())
                } else if typ.eq_ignore_ascii_case("string")
                    || typ.eq_ignore_ascii_case("str")
                    || typ.eq_ignore_ascii_case("varchar")
                    || is_untyped_container
                {
                    better.unwrap_or_else(|| typ.clone())
                } else {
                    typ.clone()
                };
                (name.clone(), final_typ)
            })
            .collect();
        schema = refined;
    }
    // #1274: Do not force later columns to String for names-only dict rows. Use inferred types
    // for all columns so collect() returns numeric when data is numeric (PySpark-aligned semantics:
    // when schema says String, Row returns string; infer numeric so schema is not String).

    Ok((rows, schema, schema_was_inferred, mixed_row_kinds))
}

/// PySpark parity (#711, #270): tuple/list row length must match explicit schema field count.
/// When `allow_extra` (#1642), rows may have more values than names-only schema columns.
pub(super) fn validate_tuple_list_row_length(
    row_idx: usize,
    got: usize,
    expected: usize,
    allow_extra: bool,
) -> PyResult<()> {
    if got < expected || (got > expected && !allow_extra) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "createDataFrame: LENGTH_SHOULD_BE_THE_SAME. length should be the same. Expected {} fields, got {} (row index {}).",
            expected, got, row_idx
        )));
    }
    Ok(())
}

pub(super) fn python_row_to_json(
    py: Python<'_>,
    item: &Bound<'_, PyAny>,
    row_idx: usize,
    column_order: Option<&[String]>,
    from_pandas: bool,
    allow_scalar_single_column: bool,
    allow_extra_seq_fields: bool,
) -> PyResult<Vec<JsonValue>> {
    // When column_order is given: use Python items() so key/value come from Python (#1267).
    if let Some(order) = column_order {
        // Tuple/list rows map by position to schema columns (PySpark parity; #1544).
        if let Ok(tup) = item.cast::<pyo3::types::PyTuple>() {
            validate_tuple_list_row_length(
                row_idx,
                tup.len(),
                order.len(),
                allow_extra_seq_fields,
            )?;
            let field_count = if allow_extra_seq_fields {
                tup.len()
            } else {
                order.len()
            };
            let mut values = Vec::with_capacity(field_count);
            for i in 0..field_count {
                let v = tup
                    .get_item(i)
                    .ok()
                    .and_then(|pv| py_any_to_json(py, &pv).ok())
                    .unwrap_or(JsonValue::Null);
                values.push(v);
            }
            return Ok(values);
        }
        if let Ok(seq) = item.cast::<PyList>() {
            validate_tuple_list_row_length(
                row_idx,
                seq.len(),
                order.len(),
                allow_extra_seq_fields,
            )?;
            let field_count = if allow_extra_seq_fields {
                seq.len()
            } else {
                order.len()
            };
            let mut values = Vec::with_capacity(field_count);
            for i in 0..field_count {
                let v = seq
                    .get_item(i)
                    .ok()
                    .and_then(|pv| py_any_to_json(py, &pv).ok())
                    .unwrap_or(JsonValue::Null);
                values.push(v);
            }
            return Ok(values);
        }
        if let Ok(items_view) = item.call_method0("items") {
            if let Ok(list_fn) = py.import("builtins").and_then(|b| b.getattr("list")) {
                let list_result = list_fn.call1((items_view,));
                if let Ok(any_list) = list_result {
                    if let Ok(pairs) = any_list.cast::<PyList>() {
                        let mut map: Vec<(String, JsonValue)> = Vec::with_capacity(pairs.len());
                        for pair in pairs.iter() {
                            if let Ok(tup) = pair.cast::<PyTuple>() {
                                if tup.len() == 2 {
                                    let key_str = tup
                                        .get_item(0)
                                        .ok()
                                        .and_then(|pk| pk.extract::<String>().ok())
                                        .unwrap_or_else(|| {
                                            tup.get_item(0)
                                                .map(|pk| pk.to_string())
                                                .unwrap_or_default()
                                        });
                                    // Normalize key: PyO3 to_string() can return repr form (e.g. 'dept') so lookup by "dept" fails (#1267, #357).
                                    let key_str = key_str
                                        .strip_prefix('\'')
                                        .and_then(|s| s.strip_suffix('\''))
                                        .map(|s| s.to_string())
                                        .unwrap_or(key_str);
                                    let v = tup
                                        .get_item(1)
                                        .ok()
                                        .and_then(|pv| py_any_to_json(py, &pv).ok())
                                        .unwrap_or(JsonValue::Null);
                                    map.push((key_str, v));
                                }
                            }
                        }
                        if !map.is_empty() {
                            let mut values = Vec::with_capacity(order.len());
                            for k in order {
                                let val = map
                                    .iter()
                                    .find(|(key, _)| key == k || key.eq_ignore_ascii_case(k))
                                    .map(|(_, v)| v.clone());
                                values.push(val.unwrap_or(JsonValue::Null));
                            }
                            return Ok(values);
                        }
                    }
                }
            }
        }
        if let Ok(dict) = item.cast::<PyDict>() {
            let mut values = Vec::with_capacity(order.len());
            for k in order {
                // Use Python equality for key match so dict lookup works regardless of key type (#1267).
                let val = dict
                    .iter()
                    .find(|(pk, _)| pk.extract::<String>().map(|s| s == *k).unwrap_or(false))
                    .and_then(|(_, pv)| py_any_to_json(py, &pv).ok());
                let val = val.or_else(|| {
                    dict.iter()
                        .find(|(pk, _)| {
                            pk.extract::<String>()
                                .map(|s| s.eq_ignore_ascii_case(k))
                                .unwrap_or(false)
                        })
                        .and_then(|(_, pv)| py_any_to_json(py, &pv).ok())
                });
                values.push(val.unwrap_or(JsonValue::Null));
            }
            return Ok(values);
        }
        if let Ok(get_fn) = item.getattr("get") {
            if get_fn.is_callable() {
                let mut values = Vec::with_capacity(order.len());
                for k in order {
                    let py_key = pyo3::types::PyString::new(py, k.as_str());
                    let v = item
                        .call_method1("get", (py_key,))
                        .ok()
                        .and_then(|pv| py_any_to_json(py, &pv).ok());
                    values.push(v.unwrap_or(JsonValue::Null));
                }
                return Ok(values);
            }
        }
    }
    if let Ok(dict) = item.cast::<PyDict>() {
        // No column order: build key -> value map from iter(), then sort keys (or preserve pandas order).
        // (When column_order is Some we already returned above via get().)
        let mut map: Vec<(String, JsonValue)> = Vec::with_capacity(dict.len());
        for (pk, pv) in dict.iter() {
            let key_str = pk.extract::<String>().unwrap_or_else(|_| {
                pk.to_string()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string()
            });
            map.push((key_str, py_any_to_json(py, &pv)?));
        }
        let mut keys: Vec<String> = map.iter().map(|(s, _)| s.clone()).collect();
        if !from_pandas {
            keys.sort();
        }
        let mut ordered = Vec::with_capacity(keys.len());
        for k in &keys {
            let v = map
                .iter()
                .find(|(key, _)| key == k || key.eq_ignore_ascii_case(k))
                .map(|(_, val)| val.clone())
                .unwrap_or(JsonValue::Null);
            ordered.push(v);
        }
        return Ok(ordered);
    }
    if let Ok(seq) = item.cast::<PyList>() {
        let mut values = Vec::with_capacity(seq.len());
        for v in seq.iter() {
            values.push(py_any_to_json(py, &v)?);
        }
        return Ok(values);
    }
    if let Ok(tup) = item.cast::<pyo3::types::PyTuple>() {
        let mut values = Vec::with_capacity(tup.len());
        for v in tup.iter() {
            values.push(py_any_to_json(py, &v)?);
        }
        return Ok(values);
    }
    // Scalar row: allow when schema is explicit single-column (e.g. createDataFrame([1,2,3], "bigint")).
    if allow_scalar_single_column && column_order.map(|o| o.len() == 1).unwrap_or(false) {
        return Ok(vec![py_any_to_json(py, item)?]);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
        "row {}: expected dict, list, or tuple",
        row_idx
    )))
}

/// Convert PySpark/sparkless Row (or similar) to JSON object for nested struct columns (#1597).
pub(super) fn py_row_like_to_json(
    py: Python<'_>,
    v: &Bound<'_, PyAny>,
) -> Option<PyResult<JsonValue>> {
    if let Ok(as_dict) = v.getattr("asDict") {
        if as_dict.is_callable() {
            if let Ok(d) = as_dict.call0() {
                if let Ok(dict) = d.cast::<PyDict>() {
                    return Some((|| {
                        let mut obj = serde_json::Map::new();
                        for (k, val) in dict.iter() {
                            let key = k.extract::<String>().unwrap_or_else(|_| k.to_string());
                            obj.insert(key, py_any_to_json(py, &val)?);
                        }
                        Ok(JsonValue::Object(obj))
                    })());
                }
            }
        }
    }
    if let Ok(fields_attr) = v.getattr("_fields") {
        if let Ok(fields_list) = fields_attr.cast::<PyList>() {
            if !fields_list.is_empty() {
                return Some((|| {
                    let mut obj = serde_json::Map::new();
                    for (i, field_py) in fields_list.iter().enumerate() {
                        let key = field_py.extract::<String>()?;
                        let val_py = v.get_item(i)?;
                        obj.insert(key, py_any_to_json(py, &val_py)?);
                    }
                    Ok(JsonValue::Object(obj))
                })());
            }
        }
    }
    None
}

pub(super) fn py_any_to_json(_py: Python<'_>, v: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if v.is_none() {
        return Ok(JsonValue::Null);
    }
    // Check for pandas NA/NaT (pd.NA, pd.NaT) which should be treated as null.
    // These have a type name containing "NA" or "NaT" and are not extractable as normal types.
    let type_name = v
        .get_type()
        .name()
        .map(|s| s.to_string())
        .unwrap_or_default();
    if type_name == "NAType" || type_name == "NaTType" {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = v.extract::<bool>() {
        return Ok(JsonValue::Bool(b));
    }
    if type_name == "Decimal" {
        return Ok(JsonValue::String(format!(
            "__sparkless_decimal__:{}",
            v.str()?
        )));
    }
    // Try String before i64/f64 so createDataFrame([{"key": "3"}]) keeps "3" as string (Issue #1262).
    // PyO3 can extract str "3" as i64, which would wrongly infer key as long and break union type coercion.
    if let Ok(s) = v.extract::<String>() {
        const DECIMAL_TRANSPORT_PREFIX: &str = "__sparkless_decimal__:";
        return Ok(JsonValue::String(
            if s.starts_with(DECIMAL_TRANSPORT_PREFIX) {
                format!("{DECIMAL_TRANSPORT_PREFIX}{s}")
            } else {
                s
            },
        ));
    }
    if let Ok(i) = v.extract::<i64>() {
        return Ok(JsonValue::Number(serde_json::Number::from(i)));
    }
    if let Ok(f) = v.extract::<f64>() {
        if f.is_nan() {
            return Ok(JsonValue::String("NaN".to_string()));
        }
        if f.is_infinite() {
            return Ok(JsonValue::String(if f.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            }));
        }
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Ok(JsonValue::Number(n));
        }
    }
    if let Ok(bytes) = v.cast::<PyBytes>() {
        use ::base64::Engine;
        let b: &[u8] = bytes.as_bytes();
        let encoded = ::base64::engine::general_purpose::STANDARD.encode(b);
        return Ok(JsonValue::String(encoded));
    }
    if let Ok(list) = v.cast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_any_to_json(_py, &item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(dict) = v.cast::<PyDict>() {
        let mut obj = serde_json::Map::new();
        for (k, val) in dict.iter() {
            let key = k.extract::<String>().unwrap_or_else(|_| k.to_string());
            obj.insert(key, py_any_to_json(_py, &val)?);
        }
        return Ok(JsonValue::Object(obj));
    }
    if let Some(result) = py_row_like_to_json(_py, v) {
        return result;
    }
    if let Ok(tup) = v.cast::<PyTuple>() {
        let mut arr = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            arr.push(py_any_to_json(_py, &item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    Ok(JsonValue::String(v.to_string()))
}

pub(super) fn infer_schema_from_first_row(
    _py: Python<'_>,
    item: &Bound<'_, PyAny>,
    from_pandas: bool,
) -> Option<Vec<(String, String)>> {
    if let Ok(dict) = item.cast::<PyDict>() {
        let mut keys: Vec<String> = dict
            .keys()
            .iter()
            .filter_map(|k| k.extract::<String>().ok())
            .collect();
        if !from_pandas {
            keys.sort();
        }
        let mut out = Vec::with_capacity(keys.len());
        for k in &keys {
            let typ = dict
                .get_item(k.as_str())
                .ok()
                .flatten()
                .map(|v| infer_type_from_py_value(&v))
                .unwrap_or_else(|| "string".to_string());
            out.push((k.clone(), typ));
        }
        return Some(out);
    }
    // Namedtuple (and tuple-like with _fields): use _fields as column names so collect() returns row["a"], row["b"] (PySpark parity).
    let fields_attr = item.getattr("_fields").ok()?;
    let n: usize = fields_attr.len().ok()?.try_into().ok()?;
    let mut names: Vec<String> = Vec::with_capacity(n);
    for i in 0..n {
        let f = fields_attr.get_item(i).ok()?;
        let s = f.extract::<String>().ok()?;
        names.push(s);
    }
    let mut out = Vec::with_capacity(names.len());
    for (i, name) in names.iter().enumerate() {
        let val = item.get_item(i).ok()?;
        let typ = infer_type_from_py_value(&val);
        out.push((name.clone(), typ));
    }
    Some(out)
}

/// Element type inside `array<...>`, or empty string for untyped `array` / `list`.
pub(super) fn parse_array_element_type_name(s: &str) -> Option<String> {
    let trimmed = s.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower == "array" || lower == "list" {
        return Some(String::new());
    }
    if lower.starts_with("array<") && lower.ends_with('>') {
        return Some(trimmed[6..trimmed.len() - 1].trim().to_string());
    }
    None
}

pub(super) fn format_array_type_name(elem: &str) -> String {
    if elem.trim().is_empty() {
        "array".to_string()
    } else {
        format!("array<{}>", elem.trim())
    }
}

pub(super) fn merge_spark_type_names(a: &str, b: &str) -> String {
    let a = a.trim().to_ascii_lowercase();
    let b = b.trim().to_ascii_lowercase();
    if a == b {
        return a;
    }
    fn format_decimal_type_name(integral_digits: usize, scale: usize) -> String {
        // Keep integral capacity and reduce scale as needed to fit Spark's
        // maximum decimal precision of 38.
        let scale = scale.min(38usize.saturating_sub(integral_digits));
        let precision = integral_digits.saturating_add(scale).clamp(1, 38);
        format!("decimal({precision},{scale})")
    }
    let decimal_a = parse_decimal_type_name(&a);
    let decimal_b = parse_decimal_type_name(&b);
    if let (Some((precision_a, scale_a)), Some((precision_b, scale_b))) = (decimal_a, decimal_b) {
        let scale = scale_a.max(scale_b);
        let integral_digits = precision_a
            .saturating_sub(scale_a)
            .max(precision_b.saturating_sub(scale_b));
        return format_decimal_type_name(integral_digits, scale);
    }
    fn integer_precision(type_name: &str) -> Option<usize> {
        match type_name {
            "tinyint" | "byte" => Some(3),
            "smallint" | "short" | "int16" => Some(5),
            "int" | "integer" | "int32" => Some(10),
            // The largest absolute signed 64-bit value has 19 decimal digits.
            "long" | "bigint" | "int64" => Some(19),
            "uint8" => Some(3),
            "uint16" => Some(5),
            "uint32" => Some(10),
            "uint64" => Some(20),
            _ => None,
        }
    }
    if let (Some((precision, scale)), Some(integer)) = (decimal_a, integer_precision(&b)) {
        let integral_digits = precision.saturating_sub(scale).max(integer);
        return format_decimal_type_name(integral_digits, scale);
    }
    if let (Some(integer), Some((precision, scale))) = (integer_precision(&a), decimal_b) {
        let integral_digits = integer.max(precision.saturating_sub(scale));
        return format_decimal_type_name(integral_digits, scale);
    }
    if (decimal_a.is_some() && matches!(b.as_str(), "double" | "float"))
        || (decimal_b.is_some() && matches!(a.as_str(), "double" | "float"))
    {
        return "double".to_string();
    }
    if let (Some(ea), Some(eb)) = (
        parse_array_element_type_name(&a),
        parse_array_element_type_name(&b),
    ) {
        if ea.is_empty() && !eb.is_empty() {
            return format_array_type_name(&eb);
        }
        if eb.is_empty() && !ea.is_empty() {
            return format_array_type_name(&ea);
        }
        if ea.is_empty() && eb.is_empty() {
            return "array".to_string();
        }
        return format_array_type_name(&merge_spark_type_names(&ea, &eb));
    }
    if let Some(eb) = parse_array_element_type_name(&b) {
        if a == "string" {
            return format_array_type_name(&eb);
        }
    }
    if let Some(ea) = parse_array_element_type_name(&a) {
        if b == "string" {
            return format_array_type_name(&ea);
        }
    }
    if a == "string" || b == "string" {
        return "string".to_string();
    }
    if (a == "long" || a == "bigint" || a == "int") && (b == "double" || b == "float") {
        return "double".to_string();
    }
    if (b == "long" || b == "bigint" || b == "int") && (a == "double" || a == "float") {
        return "double".to_string();
    }
    if (a == "long" || a == "bigint" || a == "int") && (b == "long" || b == "bigint" || b == "int")
    {
        return "long".to_string();
    }
    "string".to_string()
}

pub(super) fn parse_decimal_type_name(type_name: &str) -> Option<(usize, usize)> {
    let args = type_name
        .trim()
        .strip_prefix("decimal(")?
        .strip_suffix(')')?;
    let (precision, scale) = args.split_once(',')?;
    Some((precision.trim().parse().ok()?, scale.trim().parse().ok()?))
}

pub(super) fn merge_inferred_types<I>(types: I) -> String
where
    I: IntoIterator<Item = String>,
{
    types.into_iter().fold(String::new(), |acc, t| {
        if acc.is_empty() {
            t
        } else if t.is_empty() {
            acc
        } else {
            merge_spark_type_names(&acc, &t)
        }
    })
}

pub(super) fn infer_array_type_from_py_sequence(
    seq: &Bound<'_, PyAny>,
    first_element_only: bool,
) -> String {
    if let Ok(list) = seq.cast::<PyList>() {
        if first_element_only {
            if list.len() > 0 {
                if let Ok(first) = list.get_item(0) {
                    return format!("array<{}>", infer_type_from_py_value(&first));
                }
            }
            // Empty arrays are untyped; merge with non-empty rows to infer element type.
            return "array".to_string();
        }
        let elem = merge_inferred_types(list.iter().map(|item| infer_type_from_py_value(&item)));
        return format_array_type_name(&elem);
    }
    if let Ok(tup) = seq.cast::<PyTuple>() {
        if first_element_only {
            if tup.len() > 0 {
                if let Ok(first) = tup.get_item(0) {
                    return format!("array<{}>", infer_type_from_py_value(&first));
                }
            }
            return "array".to_string();
        }
        let elem = merge_inferred_types(tup.iter().map(|item| infer_type_from_py_value(&item)));
        return format_array_type_name(&elem);
    }
    "array".to_string()
}

pub(super) fn infer_type_from_py_value(v: &Bound<'_, PyAny>) -> String {
    let cfg = robin_sparkless::get_thread_runtime_config();
    if v.is_none() {
        return "string".to_string();
    }
    if v.get_type().name().is_ok_and(|name| name == "Decimal") {
        if let Ok(tuple) = v.call_method0("as_tuple") {
            let digits = tuple.get_item(1).ok().and_then(|value| value.len().ok());
            let exponent = tuple
                .get_item(2)
                .ok()
                .and_then(|value| value.extract::<i32>().ok());
            if let (Some(digits), Some(exponent)) = (digits, exponent) {
                let scale = exponent.saturating_neg().max(0) as usize;
                let precision = (digits as i32 + exponent.max(0)).max(scale as i32).max(1);
                return format!("decimal({precision},{scale})");
            }
        }
        return "decimal(38,18)".to_string();
    }
    if v.extract::<bool>().is_ok() {
        return "boolean".to_string();
    }
    if v.extract::<i64>().is_ok() {
        return "long".to_string();
    }
    if v.extract::<f64>().is_ok() {
        return "double".to_string();
    }
    // Nested dicts in createDataFrame rows infer as struct (Rust/json path), not map.
    if let Ok(list) = v.cast::<PyList>() {
        return infer_array_type_from_py_sequence(list, cfg.infer_array_type_from_first_element);
    }
    if let Ok(tup) = v.cast::<PyTuple>() {
        return infer_array_type_from_py_sequence(tup, cfg.infer_array_type_from_first_element);
    }
    if v.cast::<PyBytes>().is_ok() {
        return "binary".to_string();
    }
    // #1103: Preserve date/timestamp from Python so createDataFrame([{"d": date(2026,1,1), "s": "2025-06-15"}])
    // infers d as date and s as string; string column stays string in filter result.
    if let Ok(dt) = v.getattr("isoformat") {
        if dt.is_callable() {
            if v.getattr("hour").is_ok() {
                return "timestamp".to_string();
            }
            return "date".to_string();
        }
    }
    "string".to_string()
}
