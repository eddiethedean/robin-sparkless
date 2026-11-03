//! PySpark session construction, lifecycle, and Python-facing methods.

use super::*;

/// #1344: module = "sparkless.sql.session" so type(spark).__module__ identifies engine (not builtins).
#[pyclass(module = "sparkless.sql.session")]
pub(super) struct PySparkSessionBuilder {
    pub(super) inner: SparkSessionBuilder,
}

#[pymethods]
impl PySparkSessionBuilder {
    #[new]
    fn new() -> Self {
        Self {
            inner: SparkSessionBuilder::new(),
        }
    }

    #[allow(clippy::empty_line_after_outer_attr)]
    /// PySpark parity (#412, #1239): Builder is not callable; SparkSession.builder.appName(...) works,
    /// builder() raises TypeError. We do not implement __call__ so that builder() raises as in PySpark.

    fn app_name<'a>(mut slf: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.app_name(name);
        slf
    }

    /// PySpark camelCase alias for app_name
    #[pyo3(name = "appName")]
    fn app_name_camel<'a>(mut slf: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.app_name(name);
        slf
    }

    fn master<'a>(mut slf: PyRefMut<'a, Self>, master: &str) -> PyRefMut<'a, Self> {
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.master(master);
        slf
    }

    fn config<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.config(key, value);
        slf
    }

    /// PySpark builder.option(key, value); value can be str, bool, int, etc. (stringified).
    fn option<'a>(
        mut slf: PyRefMut<'a, Self>,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        let s = option_value_to_string(value)?;
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.config(key, &s);
        Ok(slf)
    }

    fn get_or_create(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Py<PySparkSession>> {
        // PySpark parity #1250: return existing singleton when builder config is compatible,
        // so test_multiple_sessions sees one session; #373: create new when builder has extra config.
        let current_thread_session = thread_active_session(py);
        if let Some(sess) = current_thread_session.as_ref() {
            let sess_ref = sess.bind(py).cast::<PySparkSession>()?;
            let sess_guard = sess_ref.borrow();
            if !sess_guard.stopped.load(std::sync::atomic::Ordering::SeqCst)
                && sess_guard.builder_config_compatible(&slf.inner)
            {
                // Restore this thread's UDF/config context when reusing its active session.
                robin_sparkless::set_thread_udf_context_full(
                    std::sync::Arc::new(sess_guard.inner.udf_registry().clone()),
                    sess_guard.inner.is_case_sensitive(),
                    sess_guard
                        .inner
                        .get_config()
                        .get("spark.sql.session.timeZone")
                        .cloned(),
                    sess_guard.inner.runtime_config().clone(),
                );
                robin_sparkless::set_thread_decimal_allow_precision_loss(
                    robin_sparkless_core::decimal_allow_precision_loss_from_session_map(
                        sess_guard.inner.get_config(),
                    ),
                );
                // Reset builder to avoid config pollution between tests.
                slf.inner = SparkSessionBuilder::new();
                return Ok(sess.clone_ref(py));
            }
        }

        // PySparkSession wraps a thread-affine Polars session. Never borrow the process-wide
        // Python singleton from another thread: create a session local to this thread instead.
        // Keep the compatibility singleton only when no other thread already owns one.
        let ty = py.get_type::<PySparkSession>();
        let singleton = ty
            .getattr("_singleton_session")
            .ok()
            .and_then(|singleton| singleton.extract::<Py<PySparkSession>>().ok());
        let register_as_singleton = singleton.as_ref().is_none_or(|global| {
            current_thread_session
                .as_ref()
                .is_some_and(|current| global.as_ptr() == current.as_ptr())
        });

        let session = slf.inner.clone().get_or_create();
        // Reset builder to avoid config pollution between tests
        slf.inner = SparkSessionBuilder::new();
        let obj = Py::new(
            py,
            PySparkSession {
                inner: session,
                external_catalog_tables: Default::default(),
                active_actions: std::sync::Arc::new(ActionRegistry::default()),
                backend_type: std::sync::Mutex::new(DEFAULT_BACKEND_TYPE.to_string()),
                stopped: std::sync::atomic::AtomicBool::new(false),
                #[cfg(feature = "delta")]
                delta_map_view_paths: std::sync::Mutex::new(std::collections::HashMap::new()),
            },
        )?;
        register_active_session(py, obj.clone_ref(py), register_as_singleton)?;
        Ok(obj)
    }

    /// PySpark camelCase alias for get_or_create
    #[pyo3(name = "getOrCreate")]
    fn get_or_create_camel(
        slf: PyRefMut<'_, Self>,
        py: Python<'_>,
    ) -> PyResult<Py<PySparkSession>> {
        Self::get_or_create(slf, py)
    }
}

/// Default backend type so tests that read session.backend_type always get a value (e.g. "robin").
const DEFAULT_BACKEND_TYPE: &str = "robin";

#[cfg(feature = "delta")]
pub(super) fn delta_global_map_view_registry(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, std::collections::HashSet<String>>>
{
    static REGISTRY: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, std::collections::HashSet<String>>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(feature = "delta")]
pub(super) fn delta_map_paths_for_catalog_table(
    session: &PySparkSession,
    name: &str,
) -> Result<std::collections::HashSet<String>, polars::prelude::PolarsError> {
    let mut result = std::collections::HashSet::new();
    if name.starts_with("global_temp.") {
        return Ok(delta_global_map_view_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
            .unwrap_or_default());
    }
    if let Some(paths) = session
        .delta_map_view_paths
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(name)
    {
        return Ok(paths.clone());
    }
    let exact_local = session.inner.saved_table_exists(name)
        || session
            .inner
            .list_temp_view_names()
            .iter()
            .any(|view| view == name);
    if !exact_local {
        let schemas = session
            .external_catalog_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(table) = schemas.get(name) {
            return Ok(external_catalog::logical_map_paths(&table.schema));
        }
    }
    if session.inner.has_temp_view(name) {
        return Ok(result);
    }
    let Some(warehouse) = session.inner.warehouse_dir() else {
        return Ok(result);
    };
    let mut candidates = Vec::new();
    if let Ok(path) = robin_sparkless_core::resolve_path_under_base(warehouse, name) {
        candidates.push(path);
    }
    if name.contains('.') {
        let nested = name.replace('.', std::path::MAIN_SEPARATOR_STR);
        if let Ok(path) = robin_sparkless_core::resolve_path_under_base(warehouse, &nested) {
            candidates.push(path);
        }
    }
    for path in candidates {
        if !path.is_dir() {
            continue;
        }
        let paths = robin_sparkless::delta::logical_map_paths(path)?;
        if !paths.is_empty() {
            result.extend(paths);
            break;
        }
    }
    Ok(result)
}

#[cfg(feature = "delta")]
pub(super) fn delta_map_paths_for_delta_read(
    session: &PySparkSession,
    name: &str,
    version: Option<i64>,
) -> Result<std::collections::HashSet<String>, polars::prelude::PolarsError> {
    let exact_local = session.inner.saved_table_exists(name)
        || session
            .inner
            .list_temp_view_names()
            .iter()
            .any(|view| view == name);
    if !exact_local {
        let tables = session
            .external_catalog_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(table) = tables.get(name) {
            let version = version.or(table.version);
            return robin_sparkless::delta::logical_map_paths_with_version(
                &table.canonical_location,
                version,
            )
            .map(|paths| paths.into_iter().collect());
        }
    }
    if name.contains('/') || name.contains('\\') || std::path::Path::new(name).exists() {
        let path = name.strip_prefix("file://").unwrap_or(name);
        return robin_sparkless::delta::logical_map_paths_with_version(path, version)
            .map(|paths| paths.into_iter().collect());
    }
    delta_map_paths_for_catalog_table(session, name)
}

#[cfg(feature = "delta")]
pub(super) fn delta_map_paths_for_sql_query(
    session: &PySparkSession,
    query: &str,
    output_columns: &[String],
) -> std::collections::HashSet<String> {
    use robin_sparkless::sql::ast::{SelectItem, SetExpr, TableFactor};

    fn path_root_and_tail(path: &str) -> Option<(String, &str)> {
        let mut escaped = false;
        let mut end = None;
        for (index, character) in path.char_indices().skip(1) {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                end = Some(index);
                break;
            }
        }
        let end = end?;
        let root: String = serde_json::from_str(&path[..=end]).ok()?;
        Some((root, &path[end + 1..]))
    }

    fn replace_path_root(path: &str, output_root: &str) -> Option<String> {
        let (_, tail) = path_root_and_tail(path)?;
        let output_root = serde_json::to_string(output_root).ok()?;
        Some(format!("{output_root}{tail}"))
    }

    fn table_columns(session: &PySparkSession, name: &str) -> Vec<String> {
        let exact_local = session.inner.saved_table_exists(name)
            || session
                .inner
                .list_temp_view_names()
                .iter()
                .any(|view| view == name);
        if !exact_local {
            if let Some(columns) = session
                .external_catalog_tables
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(name)
                .map(|table| {
                    table
                        .schema
                        .logical_fields()
                        .iter()
                        .map(|field| field.name.clone())
                        .collect()
                })
            {
                return columns;
            }
        }
        session
            .inner
            .table(name)
            .and_then(|df| df.columns())
            .unwrap_or_default()
    }

    fn table_name_for_alias(name: &robin_sparkless::sql::ast::ObjectName) -> String {
        name.0
            .iter()
            .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
            .collect::<Vec<_>>()
            .join(".")
    }

    fn join_suffix(
        left_names: &std::collections::HashSet<String>,
        right_names: &[String],
        right_keys: &std::collections::HashSet<String>,
    ) -> String {
        let overlaps = right_names
            .iter()
            .filter(|name| left_names.contains(*name) && !right_keys.contains(*name))
            .collect::<Vec<_>>();
        if overlaps.is_empty() {
            return "_right".to_string();
        }
        for index in 0.. {
            let suffix = if index == 0 {
                "_right".to_string()
            } else {
                format!("_right_{index}")
            };
            let collides = overlaps
                .iter()
                .any(|name| left_names.contains(&format!("{name}{suffix}")));
            if !collides {
                return suffix;
            }
        }
        unreachable!("a free join suffix always exists")
    }

    struct SourceTable {
        qualifier: String,
        paths: std::collections::HashSet<String>,
        output_by_source: std::collections::HashMap<String, String>,
    }

    fn identifier_parts(expr: &robin_sparkless::sql::ast::Expr) -> Option<Vec<String>> {
        use robin_sparkless::sql::ast::Expr;
        match expr {
            Expr::Identifier(name) => Some(vec![name.value.clone()]),
            Expr::CompoundIdentifier(parts) => {
                Some(parts.iter().map(|part| part.value.clone()).collect())
            }
            Expr::Nested(expr) => identifier_parts(expr),
            _ => None,
        }
    }

    fn resolve_source_reference<'a>(
        parts: &[String],
        source_tables: &'a [SourceTable],
        case_sensitive: bool,
    ) -> Option<(&'a SourceTable, String, String)> {
        if parts.is_empty() {
            return None;
        }
        let qualified = parts.len() > 1
            && source_tables.iter().any(|table| {
                if case_sensitive {
                    table.qualifier == parts[0]
                } else {
                    table.qualifier.eq_ignore_ascii_case(&parts[0])
                }
            });
        let (qualifier, root_index) = if qualified {
            (Some(parts[0].as_str()), 1)
        } else {
            (None, 0)
        };
        let nested_start = root_index + 1;
        let matching_tables = source_tables.iter().filter(|table| {
            qualifier.is_none_or(|qualifier| {
                if case_sensitive {
                    table.qualifier == qualifier
                } else {
                    table.qualifier.eq_ignore_ascii_case(qualifier)
                }
            })
        });
        for table in matching_tables {
            let Some(canonical_root) = table.output_by_source.keys().find(|name| {
                if case_sensitive {
                    *name == &parts[root_index]
                } else {
                    name.eq_ignore_ascii_case(&parts[root_index])
                }
            }) else {
                continue;
            };
            let nested_fields = &parts[nested_start..];
            let source_path = canonical_nested_source_path(
                &table.paths,
                canonical_root,
                nested_fields,
                case_sensitive,
            )
            .or_else(|| {
                let mut source_path = serde_json::to_string(canonical_root).ok()?;
                for field in nested_fields {
                    source_path.push('.');
                    source_path.push_str(&serde_json::to_string(field).ok()?);
                }
                Some(source_path)
            })?;
            return Some((table, canonical_root.clone(), source_path));
        }
        None
    }

    fn json_string_prefix(input: &str) -> Option<(String, &str)> {
        if !input.starts_with('"') {
            return None;
        }
        let mut escaped = false;
        for (index, character) in input.char_indices().skip(1) {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                let value = serde_json::from_str(&input[..=index]).ok()?;
                return Some((value, &input[index + 1..]));
            }
        }
        None
    }

    fn canonical_nested_source_path(
        paths: &std::collections::HashSet<String>,
        root: &str,
        nested_fields: &[String],
        case_sensitive: bool,
    ) -> Option<String> {
        let encoded_root = serde_json::to_string(root).ok()?;
        if nested_fields.is_empty() {
            return Some(encoded_root);
        }
        for path in paths {
            let Some((path_root, mut tail)) = path_root_and_tail(path) else {
                continue;
            };
            if path_root != root {
                continue;
            }
            let mut prefix_length = path.len() - tail.len();
            let mut matches = true;
            for expected_field in nested_fields {
                let Some(field_tail) = tail.strip_prefix('.') else {
                    matches = false;
                    break;
                };
                let Some((actual_field, remaining)) = json_string_prefix(field_tail) else {
                    matches = false;
                    break;
                };
                let same_field = if case_sensitive {
                    actual_field == *expected_field
                } else {
                    actual_field.eq_ignore_ascii_case(expected_field)
                };
                if !same_field {
                    matches = false;
                    break;
                }
                prefix_length += tail.len() - remaining.len();
                tail = remaining;
            }
            if matches {
                return Some(path[..prefix_length].to_string());
            }
        }
        None
    }

    fn tails_below_path(
        paths: &std::collections::HashSet<String>,
        source_path: &str,
    ) -> std::collections::HashSet<String> {
        paths
            .iter()
            .filter_map(|path| {
                if path == source_path {
                    Some(String::new())
                } else {
                    path.strip_prefix(source_path)
                        .filter(|tail| tail.starts_with(['.', '[', '{']))
                        .map(str::to_string)
                }
            })
            .collect()
    }

    struct SourceInput {
        qualifier: String,
        paths: std::collections::HashSet<String>,
        output_prefix: Option<String>,
        join_alias: Option<String>,
        columns: Vec<String>,
    }

    fn source_input(session: &PySparkSession, factor: &TableFactor) -> Option<SourceInput> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let table_leaf = table_name.rsplit('.').next().unwrap_or(table_name.as_str());
                let full_name = table_name_for_alias(name);
                let output_prefix = alias
                    .as_ref()
                    .map(|alias| alias.name.value.as_str())
                    .filter(|alias| *alias != full_name.as_str())
                    .map(str::to_string);
                let qualifier = alias
                    .as_ref()
                    .map(|alias| alias.name.value.as_str())
                    .unwrap_or(table_leaf)
                    .to_string();
                Some(SourceInput {
                    qualifier,
                    paths: delta_map_paths_for_catalog_table(session, &full_name).ok()?,
                    join_alias: output_prefix.clone(),
                    output_prefix,
                    columns: table_columns(session, &full_name),
                })
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let query = subquery.to_string();
                let dataframe = session.inner.sql(&query).ok()?;
                let columns = dataframe.columns().ok()?;
                Some(SourceInput {
                    qualifier: alias
                        .as_ref()
                        .map(|alias| alias.name.value.clone())
                        .unwrap_or_default(),
                    paths: delta_map_paths_for_sql_query(session, &query, &columns),
                    output_prefix: None,
                    join_alias: alias.as_ref().map(|alias| alias.name.value.clone()),
                    columns,
                })
            }
            _ => None,
        }
    }

    let Ok(robin_sparkless::sql::Statement::Query(query)) = robin_sparkless::sql::parse_sql(query)
    else {
        return Default::default();
    };
    match query.body.as_ref() {
        SetExpr::Query(inner) => {
            return delta_map_paths_for_sql_query(session, &inner.to_string(), output_columns);
        }
        SetExpr::SetOperation { left, right, .. } => {
            let left_query = left.to_string();
            let right_query = right.to_string();
            let Ok(left_df) = session.inner.sql(&left_query) else {
                return Default::default();
            };
            let Ok(right_df) = session.inner.sql(&right_query) else {
                return Default::default();
            };
            let (Ok(left_columns), Ok(right_columns)) = (left_df.columns(), right_df.columns())
            else {
                return Default::default();
            };
            let mut paths = delta_map_paths_for_sql_query(session, &left_query, &left_columns);
            let right_paths = delta_map_paths_for_sql_query(session, &right_query, &right_columns);
            for path in right_paths {
                let Some((root, tail)) = path_root_and_tail(&path) else {
                    continue;
                };
                let Some(index) = right_columns
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(&root))
                else {
                    continue;
                };
                let Some(left_name) = left_columns.get(index) else {
                    continue;
                };
                let Ok(left_root) = serde_json::to_string(left_name) else {
                    continue;
                };
                paths.insert(format!("{left_root}{tail}"));
            }
            return paths;
        }
        SetExpr::Select(_) => {}
        _ => return Default::default(),
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        unreachable!("the set expression was checked above")
    };
    let Some(from) = select.from.first() else {
        return Default::default();
    };
    let Some(first_source) = source_input(session, &from.relation) else {
        return Default::default();
    };
    let source_paths = first_source.paths.clone();
    if source_paths.is_empty() {
        // A joined table may supply the projected MAP even when the first table does not.
    }
    let mut current_output_names = std::collections::HashSet::new();
    let first_output_by_source = first_source
        .columns
        .iter()
        .map(|column| {
            let output = first_source
                .output_prefix
                .as_ref()
                .map(|prefix| format!("{prefix}_{column}"))
                .unwrap_or_else(|| column.clone());
            current_output_names.insert(output.clone());
            (column.clone(), output)
        })
        .collect();
    let mut source_tables = vec![SourceTable {
        qualifier: first_source.qualifier.clone(),
        paths: source_paths.clone(),
        output_by_source: first_output_by_source,
    }];
    let left_join_alias = first_source.join_alias.clone();
    for join in &from.joins {
        let Some(right_source) = source_input(session, &join.relation) else {
            continue;
        };
        {
            let columns = right_source.columns;
            let output_prefix = right_source.output_prefix;
            let right_columns = columns
                .iter()
                .map(|column| {
                    output_prefix
                        .as_ref()
                        .map(|prefix| format!("{prefix}_{column}"))
                        .unwrap_or_else(|| column.clone())
                })
                .collect::<Vec<_>>();
            let (left_keys, right_key_names) = robin_sparkless::sql::join_condition_to_on_columns(
                &join.join_operator,
                left_join_alias.as_deref(),
                right_source.join_alias.as_deref(),
            )
            .unwrap_or_default();
            let right_keys = right_key_names
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>();
            let suffix = join_suffix(&current_output_names, &right_columns, &right_keys);
            let coalesces_same_keys = left_keys == right_key_names
                && matches!(
                    &join.join_operator,
                    robin_sparkless::sql::ast::JoinOperator::Join(_)
                        | robin_sparkless::sql::ast::JoinOperator::Inner(_)
                        | robin_sparkless::sql::ast::JoinOperator::Left(_)
                        | robin_sparkless::sql::ast::JoinOperator::LeftOuter(_)
                        | robin_sparkless::sql::ast::JoinOperator::Right(_)
                        | robin_sparkless::sql::ast::JoinOperator::RightOuter(_)
                );
            let drops_right_columns = matches!(
                &join.join_operator,
                robin_sparkless::sql::ast::JoinOperator::LeftSemi(_)
                    | robin_sparkless::sql::ast::JoinOperator::LeftAnti(_)
                    | robin_sparkless::sql::ast::JoinOperator::Semi(_)
                    | robin_sparkless::sql::ast::JoinOperator::Anti(_)
            );
            let mut output_by_source = std::collections::HashMap::new();
            for (column, base) in columns.iter().zip(right_columns) {
                if drops_right_columns {
                    continue;
                }
                if coalesces_same_keys && right_keys.contains(&base) {
                    continue;
                }
                let output = if current_output_names.contains(&base) {
                    format!("{base}{suffix}")
                } else {
                    base
                };
                current_output_names.insert(output.clone());
                output_by_source.insert(column.clone(), output);
            }
            source_tables.push(SourceTable {
                qualifier: right_source.qualifier,
                paths: right_source.paths,
                output_by_source,
            });
        }
    }

    let mut output_paths = std::collections::HashSet::new();
    for item in &select.projection {
        let expression_and_alias = match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                let output_names = output_columns
                    .iter()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>();
                for source_table in &source_tables {
                    for path in &source_table.paths {
                        let Some((source_root, _)) = path_root_and_tail(path) else {
                            continue;
                        };
                        let Some(output_root) =
                            source_table.output_by_source.get(source_root.as_str())
                        else {
                            continue;
                        };
                        if output_names.contains(output_root) {
                            if let Some(path) = replace_path_root(path, output_root) {
                                output_paths.insert(path);
                            }
                        }
                    }
                }
                continue;
            }
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
        };
        let (expr, explicit_alias) = expression_and_alias;
        let expression = expr.to_string();
        let map_value_references =
            robin_sparkless::sql::select_expr_map_value_reference_parts(&expression)
                .ok()
                .flatten();
        let case_sensitive = session.inner.is_case_sensitive();
        if let Some(parts) = identifier_parts(expr) {
            let Some((table, canonical_root, source_path)) =
                resolve_source_reference(&parts, &source_tables, case_sensitive)
            else {
                continue;
            };
            let selected_leaf = parts.last().map(String::as_str).unwrap_or(&canonical_root);
            let mapped_output = table.output_by_source.get(&canonical_root);
            let output_name = explicit_alias
                .map(str::to_string)
                .or_else(|| {
                    output_columns
                        .iter()
                        .find(|column| column.eq_ignore_ascii_case(selected_leaf))
                        .cloned()
                })
                .or_else(|| {
                    mapped_output.and_then(|mapped| {
                        output_columns
                            .iter()
                            .find(|column| column.eq_ignore_ascii_case(mapped))
                            .cloned()
                    })
                })
                .unwrap_or_else(|| selected_leaf.to_string());
            let output_root = serde_json::to_string(&output_name).unwrap_or_default();
            for tail in tails_below_path(&table.paths, &source_path) {
                output_paths.insert(format!("{output_root}{tail}"));
            }
            continue;
        }

        // Computed expressions such as COALESCE and CASE preserve a map only
        // when every referenced branch has the same logical map paths.
        let Some(references) = map_value_references else {
            continue;
        };
        if references.is_empty() {
            continue;
        }
        let output_name = explicit_alias
            .map(str::to_string)
            .unwrap_or_else(|| expression.clone());
        let Some(output_name) = output_columns
            .iter()
            .find(|column| column.eq_ignore_ascii_case(&output_name))
        else {
            continue;
        };
        let mut common_tails: Option<std::collections::HashSet<String>> = None;
        for reference in references {
            let Some((table, _, source_path)) =
                resolve_source_reference(&reference, &source_tables, case_sensitive)
            else {
                common_tails = Some(Default::default());
                break;
            };
            let source_tails = tails_below_path(&table.paths, &source_path);
            if let Some(common_tails) = &mut common_tails {
                common_tails.retain(|tail| source_tails.contains(tail));
            } else {
                common_tails = Some(source_tails);
            }
        }
        let output_root = serde_json::to_string(output_name).unwrap_or_default();
        for tail in common_tails.unwrap_or_default() {
            output_paths.insert(format!("{output_root}{tail}"));
        }
    }
    output_paths
}

/// #1344: module = "sparkless.sql.session" so type(spark).__module__ identifies engine (not builtins).

/// Shareable cancellation control for a thread-bound SparkSession.
#[pyclass(module = "sparkless.sql.session", frozen)]
pub(super) struct PyCancellationHandle {
    active_actions: std::sync::Arc<ActionRegistry>,
}

#[pymethods]
impl PyCancellationHandle {
    fn cancel_all_active_actions(&self) -> usize {
        self.active_actions.cancel_active()
    }

    #[pyo3(name = "cancelAllActiveJobs")]
    fn cancel_all_active_jobs(&self) -> usize {
        self.cancel_all_active_actions()
    }
}

/// Give Python control threads a chance to request cancellation during long result conversion.
/// Python objects can only be built while holding the GIL, so conversion loops yield between
/// bounded batches rather than attempting to move Python values across a detached section.
pub(super) fn yield_to_python_control_threads(py: Python<'_>) {
    py.detach(std::thread::yield_now);
}

impl PySparkSession {
    /// Returns true if the builder's config is a subset of this session's config (for getOrCreate singleton check).
    fn builder_config_compatible(&self, builder: &SparkSessionBuilder) -> bool {
        let existing = self.inner.get_config();
        let builder_config = builder.get_config();
        builder_config
            .iter()
            .all(|(k, v)| existing.get(k).map(|s| s.as_str()) == Some(v.as_str()))
    }

    pub(super) fn remember_delta_temp_view(&self, name: &str, df: &PyDataFrame) {
        #[cfg(feature = "delta")]
        {
            if name.starts_with("global_temp.") {
                delta_global_map_view_registry()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(name.to_string(), df.logical_map_paths.clone());
            } else {
                self.delta_map_view_paths
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(name.to_string(), df.logical_map_paths.clone());
            }
        }
        let _ = (name, df);
    }
}

/// Return this thread's active session. PySparkSession wraps a thread-affine native session, so a
/// process-wide singleton cannot safely serve as another thread's active session.
pub(super) fn thread_active_session(py: Python<'_>) -> Option<Py<PySparkSession>> {
    THREAD_ACTIVE_SESSIONS.with(|cell| {
        let mut v = cell.borrow_mut();
        v.retain(|s| {
            let bound = s.bind(py);
            if let Ok(sess) = bound.cast::<PySparkSession>() {
                return !sess
                    .borrow()
                    .stopped
                    .load(std::sync::atomic::Ordering::SeqCst);
            }
            true
        });
        v.last().map(|s| s.clone_ref(py))
    })
}

/// Resolve the active session using PySpark's thread-local active-session semantics.
pub(super) fn resolve_active_session(py: Python<'_>) -> Option<Py<PySparkSession>> {
    thread_active_session(py)
}

pub(super) fn register_active_session(
    py: Python<'_>,
    session: Py<PySparkSession>,
    set_singleton: bool,
) -> PyResult<()> {
    let ptr = session.as_ptr();
    THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow_mut().push(session.clone_ref(py)));

    let ty = py.get_type::<PySparkSession>();
    // Ensure compatibility attrs exist and are mutable from Python.
    if ty.getattr("_active_sessions").is_err() {
        ty.setattr("_active_sessions", PyList::empty(py))?;
    }
    if ty.getattr("_singleton_session").is_err() {
        ty.setattr("_singleton_session", py.None())?;
    }

    if set_singleton {
        // Update the compatibility list only for the process-wide session. Per-thread session
        // wrappers are intentionally kept out of it because they cannot be used from other threads.
        if let Ok(list_any) = ty.getattr("_active_sessions") {
            if let Ok(list) = list_any.cast::<PyList>() {
                let already = list.iter().any(|it| it.as_ptr() == ptr);
                if !already {
                    list.append(session.clone_ref(py))?;
                }
            }
        }
        ty.setattr("_singleton_session", session)?;
    }

    Ok(())
}

pub(super) fn require_active_session(py: Python<'_>) -> PyResult<Py<PySparkSession>> {
    resolve_active_session(py).ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "operation requires an active SparkSession",
        )
    })
}

pub(super) fn unregister_active_session_by_ptr(
    py: Python<'_>,
    ptr: *mut pyo3::ffi::PyObject,
) -> PyResult<()> {
    THREAD_ACTIVE_SESSIONS.with(|cell| {
        let mut v = cell.borrow_mut();
        v.retain(|s| s.as_ptr() != ptr);
    });

    let ty = py.get_type::<PySparkSession>();
    if let Ok(list_any) = ty.getattr("_active_sessions") {
        if let Ok(list) = list_any.cast::<PyList>() {
            // Remove all matching entries.
            let mut i = 0usize;
            while i < list.len() {
                if let Ok(item) = list.get_item(i) {
                    if item.as_ptr() == ptr {
                        if list.del_item(i).is_err() {
                            i += 1;
                        }
                        continue;
                    }
                }
                i += 1;
            }
        }
    }

    // Clear singleton if it points to this session.
    if let Ok(singleton) = ty.getattr("_singleton_session") {
        if singleton.as_ptr() == ptr {
            ty.setattr("_singleton_session", py.None())?;
        }
    }

    // Restore the UDF/config context for the session that is now on top of the
    // thread-local stack.  Stopping an inner session must not leave the thread
    // with an empty context while an outer session remains active.
    let replacement =
        THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
    if let Some(session) = replacement {
        if let Ok(session_ref) = session.bind(py).cast::<PySparkSession>() {
            let session = &session_ref.borrow().inner;
            robin_sparkless::set_thread_udf_context_full(
                std::sync::Arc::new(session.udf_registry().clone()),
                session.is_case_sensitive(),
                session
                    .get_config()
                    .get("spark.sql.session.timeZone")
                    .cloned(),
                session.runtime_config().clone(),
            );
            robin_sparkless::set_thread_decimal_allow_precision_loss(
                robin_sparkless_core::decimal_allow_precision_loss_from_session_map(
                    session.get_config(),
                ),
            );
        }
    } else {
        robin_sparkless::clear_thread_udf_context();
    }

    Ok(())
}

#[pymethods]
impl PySparkSession {
    #[new]
    #[pyo3(signature = (app_name=None, master=None, **kwargs))]
    fn new(
        py: Python<'_>,
        app_name: Option<String>,
        master: Option<String>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PySparkSession>> {
        let mut builder = SparkSession::builder();
        if let Some(name) = app_name.as_deref() {
            builder = builder.app_name(name);
        }
        if let Some(m) = master.as_deref() {
            builder = builder.master(m);
        }
        if let Some(kw) = kwargs {
            for (k, v) in kw.iter() {
                let key: String = k.extract()?;
                let val: String = if let Ok(s) = v.extract::<String>() {
                    s
                } else if let Ok(b) = v.extract::<bool>() {
                    b.to_string()
                } else if let Ok(i) = v.extract::<i64>() {
                    i.to_string()
                } else if let Ok(f) = v.extract::<f64>() {
                    f.to_string()
                } else {
                    // Best-effort: string repr.
                    v.str()?.to_string()
                };
                builder = builder.config(key, val);
            }
        }
        let session = builder.get_or_create();
        let obj = Py::new(
            py,
            PySparkSession {
                inner: session,
                external_catalog_tables: Default::default(),
                active_actions: std::sync::Arc::new(ActionRegistry::default()),
                backend_type: std::sync::Mutex::new(DEFAULT_BACKEND_TYPE.to_string()),
                stopped: std::sync::atomic::AtomicBool::new(false),
                #[cfg(feature = "delta")]
                delta_map_view_paths: std::sync::Mutex::new(std::collections::HashMap::new()),
            },
        )?;
        register_active_session(py, obj.clone_ref(py), true)?;
        Ok(obj)
    }

    /// PySpark: SparkSession.builder returns a builder instance.
    /// Note: The builder is cached as a class attribute, but get_or_create() resets
    /// its config to avoid state pollution between tests.
    #[classattr]
    fn builder(_py: Python<'_>) -> PySparkSessionBuilder {
        PySparkSessionBuilder {
            inner: SparkSession::builder(),
        }
    }

    #[getter]
    fn backend_type(&self) -> String {
        self.backend_type
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[setter]
    fn set_backend_type(slf: PyRefMut<Self>, value: String) -> PyResult<()> {
        *slf.backend_type
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
        Ok(())
    }

    #[getter]
    fn app_name(&self) -> Option<String> {
        self.inner.app_name()
    }

    #[classmethod]
    #[pyo3(name = "getActiveSession")]
    fn get_active_session(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PySparkSession>>> {
        if let Some(sess) = resolve_active_session(py) {
            return Ok(Some(sess));
        }
        Ok(None)
    }

    #[pyo3(name = "newSession")]
    fn new_session(slf: PyRef<Self>, py: Python<'_>) -> PyResult<Py<PySparkSession>> {
        let session = slf.inner.new_session();
        robin_sparkless::set_thread_udf_context_full(
            std::sync::Arc::new(session.udf_registry().clone()),
            session.is_case_sensitive(),
            session
                .get_config()
                .get("spark.sql.session.timeZone")
                .cloned(),
            session.runtime_config().clone(),
        );
        robin_sparkless::set_thread_decimal_allow_precision_loss(
            robin_sparkless_core::decimal_allow_precision_loss_from_session_map(
                session.get_config(),
            ),
        );
        let backend_type = slf
            .backend_type
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let obj = Py::new(
            py,
            PySparkSession {
                inner: session,
                external_catalog_tables: Default::default(),
                active_actions: std::sync::Arc::new(ActionRegistry::default()),
                backend_type: std::sync::Mutex::new(backend_type),
                stopped: std::sync::atomic::AtomicBool::new(false),
                #[cfg(feature = "delta")]
                delta_map_view_paths: std::sync::Mutex::new(std::collections::HashMap::new()),
            },
        )?;
        register_active_session(py, obj.clone_ref(py), true)?;
        Ok(obj)
    }

    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc=None, _tb=None))]
    fn __exit__(
        slf: PyRef<Self>,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        PySparkSession::stop(slf, py)?;
        Ok(false)
    }

    #[getter]
    fn read(slf: PyRef<Self>) -> PyDataFrameReader {
        let py = slf.py();
        PyDataFrameReader::new(slf.into_py_any(py).unwrap())
    }

    #[getter]
    fn catalog(slf: PyRef<Self>) -> PyCatalog {
        let py = slf.py();
        PyCatalog {
            session: slf.into_py_any(py).unwrap(),
        }
    }

    #[getter]
    fn conf(slf: PyRef<Self>) -> PyRuntimeConfig {
        let py = slf.py();
        PyRuntimeConfig {
            session: slf.into_py_any(py).unwrap(),
        }
    }

    #[getter]
    #[pyo3(name = "sparkContext")]
    fn spark_context(slf: PyRef<Self>) -> PySparkContext {
        let py = slf.py();
        PySparkContext {
            session: slf.into_py_any(py).unwrap(),
        }
    }

    #[getter]
    fn _storage(slf: PyRef<Self>) -> PyStorage {
        let py = slf.py();
        PyStorage {
            session: slf.into_py_any(py).unwrap(),
        }
    }

    fn create_or_replace_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner
            .create_or_replace_temp_view(name, df.inner.clone());
        self.remember_delta_temp_view(name, df);
    }

    fn create_or_replace_global_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner
            .create_or_replace_global_temp_view(name, df.inner.clone());
        self.remember_delta_temp_view(&format!("global_temp.{name}"), df);
    }

    fn drop_temp_view(&self, name: &str) -> bool {
        let dropped = self.inner.drop_temp_view(name);
        #[cfg(feature = "delta")]
        self.delta_map_view_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
        dropped
    }

    fn drop_global_temp_view(&self, name: &str) -> bool {
        let dropped = self.inner.drop_global_temp_view(name);
        #[cfg(feature = "delta")]
        delta_global_map_view_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&format!("global_temp.{name}"));
        dropped
    }

    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        let mut wrapped = self
            .inner
            .table(name)
            .map(|df| PyDataFrame::wrap_with_session(df, self.active_actions.clone()))
            .map_err(to_py_err)?;
        #[cfg(feature = "delta")]
        {
            wrapped.logical_map_paths =
                delta_map_paths_for_catalog_table(self, name).map_err(to_py_err)?;
        }
        Ok(wrapped)
    }

    /// Request cancellation for every active action started through this session.
    /// Actions report cancellation only after reaching an engine safe point.
    fn cancel_all_active_actions(&self) -> usize {
        self.active_actions.cancel_active()
    }

    /// Return a thread-safe handle that can be passed to a control thread.
    fn cancellation_handle(&self) -> PyCancellationHandle {
        PyCancellationHandle {
            active_actions: self.active_actions.clone(),
        }
    }

    #[pyo3(name = "cancellationHandle")]
    fn cancellation_handle_camel(&self) -> PyCancellationHandle {
        self.cancellation_handle()
    }

    #[pyo3(name = "cancelAllActiveJobs")]
    fn cancel_all_active_jobs(&self) -> usize {
        self.cancel_all_active_actions()
    }

    /// Attach a synchronous Python catalog provider, or pass None to remove it.
    fn set_external_catalog_provider(
        &self,
        py: Python<'_>,
        provider: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let provider = provider
            .map(|provider| {
                external_catalog::PythonExternalCatalogProvider::new(
                    py,
                    provider,
                    self.external_catalog_tables.clone(),
                )
            })
            .transpose()?
            .map(external_catalog::PythonExternalCatalogProvider::into_external_catalog);
        self.external_catalog_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.inner.set_external_catalog(provider).map_err(to_py_err)
    }

    #[pyo3(name = "setExternalCatalogProvider")]
    fn set_external_catalog_provider_camel(
        &self,
        py: Python<'_>,
        provider: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.set_external_catalog_provider(py, provider)
    }

    fn sql(&self, py: Python<'_>, query: &str) -> PyResult<PyDataFrame> {
        let action = ActiveAction::new(&self.active_actions, None);
        let token = action.token.clone();
        let execution_token = token.clone();
        #[cfg(feature = "delta")]
        let is_read_only_query = matches!(
            robin_sparkless::sql::parse_sql(query),
            Ok(robin_sparkless::sql::Statement::Query(_))
        );
        let session = self.inner.clone();
        let query = query.to_string();
        let query_for_execution = query.clone();
        let dataframe = py
            .detach(move || session.sql_with_cancellation(&query_for_execution, &execution_token))
            .map_err(to_py_err)?;
        let mut wrapped = PyDataFrame::wrap_with_session(dataframe, self.active_actions.clone());
        #[cfg(feature = "delta")]
        {
            let output_columns = wrapped.inner.columns().map_err(to_py_err)?;
            wrapped.logical_map_paths =
                delta_map_paths_for_sql_query(self, &query, &output_columns);
        }
        #[cfg(feature = "delta")]
        if is_read_only_query {
            token.check_cancelled().map_err(to_py_err)?;
        }
        Ok(wrapped)
    }

    fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        let names: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        self.inner
            .create_dataframe(data, names)
            .map(|df| PyDataFrame::wrap_with_session(df, self.active_actions.clone()))
            .map_err(to_py_err)
    }

    /// createDataFrame(data, schema=None, verify_schema=True, sampling_ratio=None).
    /// data: list/tuple of dicts, list/tuple of row tuples, iterable of rows, or pandas.DataFrame.
    /// schema: optional list of (name, type_str) or list of column names (types inferred).
    /// verify_schema: when True, validate row values against schema types (PySpark parity).
    /// sampling_ratio: accepted for API parity; ignored for list/dict data.
    #[pyo3(signature = (data, schema=None, verify_schema=true, sampling_ratio=None))]
    fn create_dataframe_from_rows(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
        verify_schema: bool,
        sampling_ratio: Option<f64>,
    ) -> PyResult<PyDataFrame> {
        let _ = sampling_ratio; // no-op for list/dict data; PySpark uses for CSV inference
                                // #419: createDataFrame([1,2,3], "bigint") — single dtype schema with scalar list.
        if let Some(schema_obj) = schema {
            if let Some(pairs) = parse_schema_from_py(py, schema_obj)? {
                if pairs.len() == 1 && pairs[0].0 == "value" {
                    if let Ok(list) = data.cast::<PyList>() {
                        let all_scalars = list.iter().all(|item| {
                            item.cast::<PyDict>().is_err()
                                && item.cast::<PyList>().is_err()
                                && item.cast::<PyTuple>().is_err()
                        });
                        if all_scalars && !list.is_empty() {
                            let mut values = Vec::with_capacity(list.len());
                            for item in list.iter() {
                                values.push(py_any_to_json(py, &item)?);
                            }
                            let df = self
                                .inner
                                .create_dataframe_from_single_column(values, &pairs[0].1)
                                .map_err(to_py_err)?;
                            let mut wrapped =
                                PyDataFrame::wrap_with_session(df, self.active_actions.clone());
                            wrapped.schema_cache = schema
                                .and_then(|s| s.getattr("fields").ok().map(|_| s.clone().unbind()));
                            return Ok(wrapped);
                        }
                    }
                }
            }
        }
        // PyArrow's Python rows contain Decimal values, but the table schema is the
        // authoritative source for precision and scale. Feed it through the same schema path.
        let mut schema_names_only: Option<Vec<String>> = schema.and_then(|schema| {
            let names = schema.cast::<PyList>().ok()?;
            names
                .iter()
                .map(|name| name.extract::<String>().ok())
                .collect()
        });
        let arrow_schema_source = if is_pyarrow_table_or_batch(data) {
            data.getattr("schema").ok()
        } else {
            None
        };
        // Match the names-only schema behavior for tuple rows: infer names for
        // extra Arrow fields, and reject a schema that names nonexistent fields.
        if let (Some(names), Some(source_schema)) =
            (schema_names_only.as_mut(), arrow_schema_source.as_ref())
        {
            let source_names = arrow_schema_field_names(source_schema)?;
            if names.len() > source_names.len() {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "[AXIS_LENGTH_MISMATCH] schema has {} column names but Arrow input has {} columns",
                    names.len(),
                    source_names.len(),
                )));
            }
            let mut fallback_index = names.len() + 1;
            while names.len() < source_names.len() {
                let fallback_name = loop {
                    let candidate = format!("_{fallback_index}");
                    fallback_index += 1;
                    if !names.iter().any(|name| name == &candidate) {
                        break candidate;
                    }
                };
                names.push(fallback_name);
            }
        }
        // Keep the Arrow logical shape when schema is omitted or only supplies
        // column names. A typed Spark schema remains authoritative.
        let keep_arrow_logical_shape = schema.is_none() || schema_names_only.is_some();
        let mut arrow_array_struct_paths = if keep_arrow_logical_shape {
            arrow_schema_source
                .as_ref()
                .map(arrow_array_struct_paths)
                .transpose()?
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let mut logical_map_paths = if keep_arrow_logical_shape {
            arrow_schema_source
                .as_ref()
                .map(arrow_logical_map_paths)
                .transpose()?
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let arrow_schema_cache = arrow_schema_source.as_ref().map(|s| s.clone().unbind());
        let arrow_schema = (schema.is_none() || schema_names_only.is_some())
            .then_some(arrow_schema_source.as_ref())
            .flatten()
            .and_then(|arrow_schema| {
                let arrow_rows_obj = data.call_method0("to_pylist").ok()?;
                let arrow_rows = arrow_rows_obj.cast::<PyList>().ok()?;
                let out = PyList::empty(py);
                for field in arrow_schema.try_iter().ok()? {
                    let field = field.ok()?;
                    let name = field.getattr("name").ok()?.extract::<String>().ok()?;
                    let arrow_type = field.getattr("type").ok()?;
                    let spark_type = if let Some(spark_type) =
                        arrow_type_to_spark_type(&arrow_type).ok().flatten()
                    {
                        spark_type
                    } else {
                        let mut inferred: Option<String> = None;
                        for row in arrow_rows.iter() {
                            let value = row.get_item(name.as_str()).ok()?;
                            if !value.is_none() {
                                let inferred_type = infer_type_from_py_value(&value);
                                inferred = Some(match inferred {
                                    Some(existing) => {
                                        merge_spark_type_names(&existing, &inferred_type)
                                    }
                                    None => inferred_type,
                                });
                            }
                        }
                        inferred.unwrap_or_else(|| "string".to_string())
                    };
                    out.append((name, spark_type)).ok()?;
                }
                Some(out.into_any())
            });
        // A names-only schema changes order/names but PyArrow still owns the
        // field types. Rebuild those typed pairs in the requested order.
        let arrow_name_schema = if let (Some(names), Some(arrow_schema)) =
            (schema_names_only.as_ref(), arrow_schema.as_ref())
        {
            let source_types = parse_schema_from_py(py, arrow_schema)?.unwrap_or_default();
            let pairs = PyList::empty(py);
            for (name, (_, type_name)) in names.iter().zip(source_types.iter()) {
                pairs.append((name.as_str(), type_name))?;
            }
            Some(pairs.into_any())
        } else {
            None
        };
        let effective_schema = arrow_name_schema
            .as_ref()
            .or(schema)
            .or(arrow_schema.as_ref());
        let (data, from_pandas, pandas_column_order) =
            normalize_create_dataframe_input(py, data, effective_schema)?;
        let data = if let (Some(names), Some(source_schema)) =
            (schema_names_only.as_ref(), arrow_schema_source.as_ref())
        {
            remap_arrow_paths_for_names(&mut arrow_array_struct_paths, source_schema, names)?;
            remap_arrow_paths_for_names(&mut logical_map_paths, source_schema, names)?;
            rename_arrow_rows_by_position(py, &data, source_schema, names)?
        } else {
            data
        };
        let schema_cache =
            schema.and_then(|s| s.getattr("fields").ok().map(|_| s.clone().unbind()));
        let (rows, schema_pairs, schema_was_inferred, mixed_row_kinds) = python_data_and_schema(
            py,
            &data,
            effective_schema,
            from_pandas,
            pandas_column_order.as_deref(),
        )?;
        let mut logical_map_columns = std::collections::HashSet::new();
        logical_map_paths.extend(spark_schema_logical_map_paths(&schema_pairs));
        arrow_array_struct_paths.extend(spark_schema_array_struct_paths(&schema_pairs));
        for (name, type_name) in &schema_pairs {
            let type_name = type_name.trim().to_ascii_lowercase();
            if type_name.starts_with("map<") {
                logical_map_columns.insert(name.clone());
                logical_map_paths.insert(arrow_field_path("", name));
            }
        }
        let df = self
            .inner
            .create_dataframe_from_rows(rows, schema_pairs, verify_schema, schema_was_inferred)
            .map_err(to_py_err)?;
        let mut wrapped = PyDataFrame::wrap_with_session(df, self.active_actions.clone());
        wrapped.schema_cache = schema_cache;
        wrapped.mixed_row_shape_error_on_collect = mixed_row_kinds && effective_schema.is_none();
        wrapped.arrow_schema_cache = arrow_schema_cache;
        wrapped.arrow_array_struct_paths = arrow_array_struct_paths;
        wrapped.logical_map_columns = logical_map_columns;
        wrapped.logical_map_paths = logical_map_paths;
        Ok(wrapped)
    }

    /// PySpark alias: createDataFrame -> create_dataframe_from_rows. Accepts verify_schema or verifySchema, sampling_ratio or samplingRatio.
    #[pyo3(name = "createDataFrame", signature = (data, schema=None, **kwargs))]
    fn create_data_frame_camel(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyDataFrame> {
        let verify_schema = extract_verify_schema(kwargs)?;
        let sampling_ratio = extract_sampling_ratio(kwargs)?;
        self.create_dataframe_from_rows(py, data, schema, verify_schema, sampling_ratio)
    }

    /// PySpark-compatible range:
    /// - range(end)
    /// - range(start, end)
    /// - range(start, end, step)
    #[pyo3(signature = (start, end=None, step=None))]
    fn range(&self, start: i64, end: Option<i64>, step: Option<i64>) -> PyResult<PyDataFrame> {
        let (start, end, step) = match (end, step) {
            (None, None) => (0, start, 1),
            (Some(e), None) => (start, e, 1),
            (Some(e), Some(s)) => (start, e, s),
            (None, Some(_)) => {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "range(end, step) is not supported; use range(start, end, step)",
                ));
            }
        };
        self.inner
            .range(start, end, step)
            .map(|df| PyDataFrame::wrap_with_session(df, self.active_actions.clone()))
            .map_err(to_py_err)
    }

    fn read_csv(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_csv(Path::new(path))
            .map(|df| PyDataFrame::wrap_with_session(df, self.active_actions.clone()))
            .map_err(to_py_err)
    }

    fn read_parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_parquet(Path::new(path))
            .map(|df| PyDataFrame::wrap_with_session(df, self.active_actions.clone()))
            .map_err(to_py_err)
    }

    fn read_json(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_json(Path::new(path))
            .map(|df| PyDataFrame::wrap_with_session(df, self.active_actions.clone()))
            .map_err(to_py_err)
    }

    fn read_delta(&self, name_or_path: &str) -> PyResult<PyDataFrame> {
        #[cfg_attr(not(feature = "delta"), allow(unused_mut))]
        let mut frame = self
            .inner
            .read_delta(name_or_path)
            .map(|df| PyDataFrame::wrap_with_session(df, self.active_actions.clone()))
            .map_err(to_py_err)?;
        #[cfg(feature = "delta")]
        {
            frame.logical_map_paths =
                delta_map_paths_for_delta_read(self, name_or_path, None).map_err(to_py_err)?;
        }
        Ok(frame)
    }

    #[pyo3(signature = (name_or_path, version=None))]
    fn read_delta_with_version(
        &self,
        name_or_path: &str,
        version: Option<i64>,
    ) -> PyResult<PyDataFrame> {
        #[cfg_attr(not(feature = "delta"), allow(unused_mut))]
        let mut frame = self
            .inner
            .read_delta_with_version(name_or_path, version)
            .map(|df| PyDataFrame::wrap_with_session(df, self.active_actions.clone()))
            .map_err(to_py_err)?;
        #[cfg(feature = "delta")]
        {
            frame.logical_map_paths =
                delta_map_paths_for_delta_read(self, name_or_path, version).map_err(to_py_err)?;
        }
        Ok(frame)
    }

    fn stop(slf: PyRef<Self>, py: Python<'_>) -> PyResult<()> {
        slf.stopped.store(true, std::sync::atomic::Ordering::SeqCst);
        slf.active_actions.cancel_active();
        slf.set_external_catalog_provider(py, None)?;
        WINDOW_ORDER_SORT_HINT.with(|cell| *cell.borrow_mut() = None);
        slf.inner.stop();
        let ptr = slf.into_py_any(py)?.as_ptr();
        unregister_active_session_by_ptr(py, ptr)?;
        Ok(())
    }
}
