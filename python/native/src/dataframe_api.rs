//! Python DataFrame methods exposed by the native binding.

use super::*;

#[pymethods]
impl PyDataFrame {
    fn filter(
        slf: PyRef<Self>,
        py: Python<'_>,
        condition: &Bound<'_, PyAny>,
    ) -> PyResult<PyDataFrame> {
        // (UDF column op literal): add UDF result column, filter by (temp op literal), drop temp.
        if let Ok(udf_filter) = condition.cast::<PyUdfFilterColumn>() {
            let udf_column_py: Py<PyColumn> = udf_filter.getattr("udf_column")?.extract()?;
            let other_json: String = udf_filter.getattr("other_json")?.extract()?;
            let col_ref = udf_column_py.bind(py).borrow();
            let (udf_name, mut arg_names, literal_jsons): (
                String,
                Vec<String>,
                Vec<Option<String>>,
            ) = match col_ref.inner.udf_call_info_with_literals() {
                Some(x) => x,
                None => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "UdfFilterColumn requires a UDF column",
                    ));
                }
            };
            let mut df_for_udf = slf.inner.clone();
            if let Some((_, args)) = col_ref.inner.udf_call_with_args() {
                for (i, arg) in args.iter().enumerate() {
                    if literal_jsons
                        .get(i)
                        .and_then(|o: &Option<String>| o.as_ref())
                        .is_some()
                    {
                        continue;
                    }
                    let columns = df_for_udf.columns().map_err(to_py_err)?;
                    let name_in_df = columns
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(arg_names[i].as_str()));
                    if !name_in_df {
                        let temp_name = format!("__udf_arg_{}", i);
                        let resolved = df_for_udf
                            .resolve_expr_column_names(arg.expr().clone())
                            .map_err(to_py_err)?;
                        df_for_udf = df_for_udf
                            .with_column_expr(&temp_name, resolved)
                            .map_err(to_py_err)?;
                        arg_names[i] = temp_name;
                    }
                }
            }
            let temp_name = format!("_udf_filter_{}", uuid::Uuid::new_v4().simple());
            let arg_names_py = PyList::new(py, arg_names.iter())?;
            let literals_py = PyList::empty(py);
            for opt in &literal_jsons {
                let item: Bound<'_, PyAny> = match opt {
                    Some(s) => pyo3::types::PyString::new(py, s.as_str()).into_any(),
                    None => py.None().into_bound(py),
                };
                literals_py.append(item)?;
            }
            let df_for_callback = slf.wrap_derived(df_for_udf);
            let df_with_col = PYTHON_UDF_EXECUTOR.with(|cell| -> PyResult<PyDataFrame> {
                let cb = cell.borrow();
                let callback = cb.as_ref().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "UDF filter requires Python UDF executor (active session)",
                    )
                })?;
                let result = callback.bind(py).call1((
                    df_for_callback,
                    &temp_name,
                    udf_name,
                    arg_names_py,
                    literals_py,
                ))?;
                let py_df = result.cast::<PyDataFrame>()?;
                let py_df = py_df.borrow();
                Ok(py_df.wrap_derived(py_df.inner.clone()))
            })?;
            let lit_expr = json_str_to_lit_expr(&other_json)?;
            let op: String = udf_filter.getattr("op")?.extract()?;
            let filter_expr = udf_filter_comparison_expr(temp_name.clone(), op.as_str(), lit_expr)?;
            let filtered = df_with_col.inner.filter(filter_expr).map_err(to_py_err)?;
            let out = filtered.drop(vec![temp_name.as_str()]).map_err(to_py_err)?;
            return Ok(df_with_col.wrap_derived(out));
        }
        if let Some(py_col) = py_any_to_py_column_ref(condition) {
            let col_ref = py_col.bind(py).borrow();
            if let Some((udf_name, arg_names, literal_jsons)) =
                col_ref.inner.udf_call_info_with_literals()
            {
                let temp_name = format!("_udf_filter_{}", uuid::Uuid::new_v4().simple());
                let arg_names_py = PyList::new(py, arg_names.iter())?;
                let literals_py = PyList::empty(py);
                for opt in &literal_jsons {
                    let item: Bound<'_, PyAny> = match opt {
                        Some(s) => pyo3::types::PyString::new(py, s.as_str()).into_any(),
                        None => py.None().into_bound(py),
                    };
                    literals_py.append(item)?;
                }
                if let Some(df_with_col) =
                    PYTHON_UDF_EXECUTOR.with(|cell| -> PyResult<Option<PyDataFrame>> {
                        let cb = cell.borrow();
                        let callback = match cb.as_ref() {
                            Some(c) => c,
                            None => return Ok(None),
                        };
                        let df_obj = slf.wrap_derived(slf.inner.clone());
                        let result = callback.bind(py).call1((
                            df_obj,
                            &temp_name,
                            udf_name,
                            arg_names_py,
                            literals_py,
                        ))?;
                        let py_df = result.cast::<PyDataFrame>()?;
                        let py_df = py_df.borrow();
                        Ok(Some(py_df.wrap_derived(py_df.inner.clone())))
                    })?
                {
                    let filter_expr = robin_sparkless::Column::new(temp_name.clone()).into_expr();
                    let filtered = df_with_col.inner.filter(filter_expr).map_err(to_py_err)?;
                    let out = filtered.drop(vec![temp_name.as_str()]).map_err(to_py_err)?;
                    return Ok(df_with_col.wrap_derived(out));
                }
            }
            let df = slf
                .inner
                .filter(col_ref.inner.clone().into_expr())
                .map(|df| slf.wrap_derived(df))
                .map_err(to_py_err)?;
            return Ok(df);
        }
        // F.expr(...) in filter: handle PyExprStr SQL expression strings.
        if let Ok(py_expr_str) = condition.cast::<PyExprStr>() {
            let session = require_active_session(py).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "filter with F.expr() requires an active SparkSession",
                )
            })?;
            let session_ref = session.bind(py).cast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            let expr = robin_sparkless::sql::expr_string_to_polars(
                &py_expr_str.borrow().sql,
                &session_ref.borrow().inner,
                &slf.inner,
            )
            .map_err(to_py_err)?;
            return slf
                .inner
                .filter(expr)
                .map(|df| slf.wrap_derived(df))
                .map_err(to_py_err);
        }
        if let Ok(expr_str) = condition.extract::<String>() {
            let session = require_active_session(py).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "filter with string expression requires an active SparkSession",
                )
            })?;
            let session_ref = session.bind(py).cast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            let expr = robin_sparkless::sql::expr_string_to_polars(
                &expr_str,
                &session_ref.borrow().inner,
                &slf.inner,
            )
            .map_err(to_py_err)?;
            return slf
                .inner
                .filter(expr)
                .map(|df| slf.wrap_derived(df))
                .map_err(to_py_err);
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "filter condition must be a Column or a string expression",
        ))
    }

    #[pyo3(name = "where")]
    fn where_(
        slf: PyRef<Self>,
        py: Python<'_>,
        condition: &Bound<'_, PyAny>,
    ) -> PyResult<PyDataFrame> {
        Self::filter(slf, py, condition)
    }

    #[pyo3(signature = (*cols))]
    fn select(&self, py: Python<'_>, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        #[derive(Debug)]
        enum Tmp {
            NameIdx(usize),
            Expr(robin_sparkless::Expr),
        }
        #[derive(Debug)]
        enum ItemOrExprStr {
            Resolved(Tmp),
            ExprStr(String),
        }

        fn push_item(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<ItemOrExprStr>,
            names: &mut Vec<Box<str>>,
            _py: Python<'_>,
        ) -> PyResult<()> {
            if let Ok(star) = item.cast::<PyStarColumns>() {
                for c in &star.borrow().columns {
                    let idx = names.len();
                    names.push(c.clone().into_boxed_str());
                    out.push(ItemOrExprStr::Resolved(Tmp::NameIdx(idx)));
                }
                return Ok(());
            }
            if let Some(col) = py_any_to_dataframe_column(item)? {
                out.push(ItemOrExprStr::Resolved(Tmp::Expr(col.into_expr())));
                return Ok(());
            }
            if let Ok(py_expr_str) = item.cast::<PyExprStr>() {
                out.push(ItemOrExprStr::ExprStr(py_expr_str.borrow().sql.clone()));
                return Ok(());
            }
            if let Ok(s) = item.extract::<String>() {
                let idx = names.len();
                names.push(s.into_boxed_str());
                out.push(ItemOrExprStr::Resolved(Tmp::NameIdx(idx)));
                return Ok(());
            }
            if let Ok(list) = item.cast::<PyList>() {
                for sub in list.iter() {
                    push_item(&sub, out, names, _py)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.cast::<PyTuple>() {
                for sub in tup.iter() {
                    push_item(&sub, out, names, _py)?;
                }
                return Ok(());
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "select() expects columns as str, Column, expr(str), or a list/tuple of those",
            ))
        }

        if cols.len() == 1 {
            // PySpark: df.select(("a", "b")) is invalid – a tuple of plain column
            // name strings is not accepted. However, tuples of Columns or other
            // expressions (e.g. posexplode/json_tuple wrappers) are valid and
            // should be flattened by select().
            let item0 = cols.get_item(0)?;
            if let Ok(tup) = item0.cast::<PyTuple>() {
                let mut all_strings = true;
                let mut has_column_like = false;
                for sub in tup.iter() {
                    if py_any_to_dataframe_column(&sub).ok().flatten().is_some()
                        || sub.cast::<PyExprStr>().is_ok()
                    {
                        has_column_like = true;
                        break;
                    }
                    if sub.extract::<String>().is_err() {
                        all_strings = false;
                        break;
                    }
                }
                if all_strings && !has_column_like {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "select() with a single tuple of column names is not supported; use a list instead (e.g. select(['a', 'b']))",
                    ));
                }
            }
        }
        let mut raw: Vec<ItemOrExprStr> = Vec::with_capacity(cols.len());
        let mut name_boxes: Vec<Box<str>> = Vec::new();
        for item in cols.iter() {
            push_item(&item, &mut raw, &mut name_boxes, py)?;
        }

        let tmp: Vec<Tmp> = if raw.iter().any(|x| matches!(x, ItemOrExprStr::ExprStr(_))) {
            let session = THREAD_ACTIVE_SESSIONS
                .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "select() with expr() requires an active SparkSession",
                    )
                })?;
            let session_ref = session.bind(py).cast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            session_ref
                .borrow()
                .inner
                .create_or_replace_temp_view("__selectexpr_t", self.inner.clone());
            let mut resolved: Vec<Tmp> = Vec::with_capacity(raw.len());
            for x in raw {
                match x {
                    ItemOrExprStr::Resolved(t) => resolved.push(t),
                    ItemOrExprStr::ExprStr(s) => {
                        let expr = robin_sparkless::sql::expr_string_to_polars(
                            &s,
                            &session_ref.borrow().inner,
                            &self.inner,
                        )
                        .map_err(to_py_err)?;
                        resolved.push(Tmp::Expr(expr));
                    }
                }
            }
            let _ = session_ref.borrow().inner.drop_temp_view("__selectexpr_t");
            resolved
        } else {
            raw.into_iter()
                .map(|x| match x {
                    ItemOrExprStr::Resolved(t) => Ok(t),
                    ItemOrExprStr::ExprStr(_) => {
                        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                            "internal: unresolved expr() string after select() rewrite",
                        ))
                    }
                })
                .collect::<PyResult<Vec<_>>>()?
        };

        // Fast-path: all arguments are simple string column names (possibly nested in lists/tuples),
        // with no Column or expr() items. In this case, delegate directly to DataFrame::select so
        // we reuse its PySpark-style ambiguous-case handling and dotted-name logic.
        if tmp.iter().all(|t| matches!(t, Tmp::NameIdx(_))) {
            let mut col_refs: Vec<&str> = Vec::with_capacity(tmp.len());
            for t in &tmp {
                if let Tmp::NameIdx(i) = t {
                    let name = name_boxes[*i].as_ref();
                    col_refs.push(name);
                }
            }
            return self
                .inner
                .select(col_refs)
                .map(|df| self.wrap_derived(df))
                .map_err(to_py_err);
        }

        let all_columns = self.inner.columns().map_err(to_py_err)?;
        // For join/crossJoin parity: when the user writes dept_df.name.alias("name_right")
        // and the current DataFrame already has a column named "name_right" (produced by
        // the join logic for duplicate right-side columns), we must resolve the alias to
        // that existing right-side column instead of re-aliasing the left "name" column.
        // We implement this as a light-weight Expr rewrite for the specific pattern
        // Alias(Column(base), alias) where alias == base + "_right" and alias exists
        // in the current DataFrame schema.
        use robin_sparkless::{Column as RsColumn, Expr as RsExpr};
        use std::collections::HashSet;
        let all_column_set: HashSet<&str> = all_columns.iter().map(|s| s.as_str()).collect();

        fn rewrite_join_right_alias(expr: RsExpr, all_column_set: &HashSet<&str>) -> RsExpr {
            match expr {
                RsExpr::Alias(inner, alias_name) => {
                    let alias_str = alias_name.as_str();
                    // Temporary debug: print alias patterns seen in select for joins.
                    // eprintln!("select alias expr: alias='{}'", alias_str);
                    if all_column_set.contains(alias_str) {
                        // Look for the specific pattern Column(base) AS "${base}_right",
                        // possibly wrapped in one or more Alias layers from Column::alias()
                        // and Column::into_expr().
                        let mut base_expr: &RsExpr = inner.as_ref();
                        while let RsExpr::Alias(inner2, _) = base_expr {
                            base_expr = inner2.as_ref();
                        }
                        if let RsExpr::Column(col_name) = base_expr {
                            let base = col_name.as_str();
                            let expected_alias = format!("{base}_right");
                            if expected_alias == alias_str {
                                // Rewrite to use the existing right-side column as the source,
                                // so we select dept_id_right/name_right from the joined frame.
                                let col = RsColumn::new(alias_str.to_string());
                                return col.into_expr();
                            }
                        }
                    }
                    RsExpr::Alias(inner, alias_name)
                }
                other => other,
            }
        }

        let mut items: Vec<SelectItem<'_>> = Vec::with_capacity(tmp.len());
        for t in tmp {
            match t {
                Tmp::Expr(e) => {
                    let rewritten = rewrite_join_right_alias(e, &all_column_set);
                    items.push(SelectItem::Expr(rewritten));
                }
                Tmp::NameIdx(i) => {
                    let name = name_boxes[i].as_ref();
                    if name == "*" {
                        for c in &all_columns {
                            items.push(SelectItem::ColumnName(c.as_str()));
                        }
                    } else {
                        items.push(SelectItem::ColumnName(name));
                    }
                }
            }
        }

        let df = self.inner.select_items(items.clone()).map_err(to_py_err)?;
        let out = WINDOW_ORDER_SORT_HINT.with(|cell| {
            if let Some((cols, desc)) = cell.borrow_mut().take() {
                // Only apply sort if this select's result has the hint columns (from row_number with no partition).
                // Otherwise the hint is stale from a previous test/select and we must not sort.
                let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
                let all_present = match df.columns() {
                    Ok(result_names) => col_refs
                        .iter()
                        .all(|c| result_names.iter().any(|n| n.eq_ignore_ascii_case(c))),
                    Err(_) => false,
                };
                if all_present {
                    let ascending: Vec<bool> = desc.iter().map(|&d| !d).collect();
                    df.order_by(col_refs, ascending)
                        .map(|df| self.wrap_select_derived(df, &items))
                } else {
                    Ok(self.wrap_select_derived(df, &items))
                }
            } else {
                Ok(self.wrap_select_derived(df, &items))
            }
        });
        out.map_err(to_py_err)
    }

    fn with_column(
        slf: PyRef<Self>,
        py: Python<'_>,
        name: &str,
        col_any: &Bound<'_, PyAny>,
    ) -> PyResult<PyDataFrame> {
        // Case 1: Column or chained when/then (PyThenBuilder) without explicit .otherwise()
        if let Some(col) = py_any_to_dataframe_column(col_any)? {
            if let Some((udf_name, mut arg_names, literal_jsons)) =
                col.udf_call_info_with_literals()
            {
                // Materialize expression args (e.g. F.col('x')*2) as temp columns so the executor can read them from the row.
                let mut df_for_udf = slf.inner.clone();
                if let Some((_, args)) = col.udf_call_with_args() {
                    for (i, arg) in args.iter().enumerate() {
                        if literal_jsons.get(i).and_then(|o| o.as_ref()).is_some() {
                            continue;
                        }
                        let columns = df_for_udf.columns().map_err(to_py_err)?;
                        let name_in_df = columns
                            .iter()
                            .any(|c| c.eq_ignore_ascii_case(arg_names[i].as_str()));
                        if !name_in_df {
                            let temp_name = format!("__udf_arg_{}", i);
                            let resolved = df_for_udf
                                .resolve_expr_column_names(arg.expr().clone())
                                .map_err(to_py_err)?;
                            df_for_udf = df_for_udf
                                .with_column_expr(&temp_name, resolved)
                                .map_err(to_py_err)?;
                            arg_names[i] = temp_name;
                        }
                    }
                }
                let arg_names_py = PyList::new(py, arg_names.iter())?;
                let literals_py = PyList::empty(py);
                for opt in &literal_jsons {
                    let item: Bound<'_, PyAny> = match opt {
                        Some(s) => pyo3::types::PyString::new(py, s.as_str()).into_any(),
                        None => py.None().into_bound(py),
                    };
                    literals_py.append(item)?;
                }
                let df_for_callback = slf.wrap_derived(df_for_udf);
                if let Some(result_df) =
                    PYTHON_UDF_EXECUTOR.with(|cell| -> PyResult<Option<PyDataFrame>> {
                        let cb = cell.borrow();
                        let callback = match cb.as_ref() {
                            Some(c) => c,
                            None => return Ok(None),
                        };
                        let result = callback.bind(py).call1((
                            df_for_callback,
                            name,
                            udf_name,
                            arg_names_py,
                            literals_py,
                        ))?;
                        let py_df = result.cast::<PyDataFrame>()?;
                        let py_df = py_df.borrow();
                        Ok(Some(py_df.wrap_derived(py_df.inner.clone())))
                    })?
                {
                    return Ok(result_df);
                }
            }
            let df = slf.inner.with_column(name, &col).map_err(|e| {
                let msg = e.to_string();
                if msg.contains("to_timestamp requires StringType") {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg)
                } else {
                    to_py_err(msg)
                }
            })?;
            // Force schema inference now so type errors surface at withColumn time (PySpark parity).
            df.schema_engine().map_err(|e| {
                let msg = e.to_string();
                if msg.contains("to_timestamp requires StringType") {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg)
                } else {
                    to_py_err(msg)
                }
            })?;
            return Ok(slf.wrap_with_column_derived(df, name, col.expr()));
        }

        // Case 2: expr-string from F.expr(...)
        if let Ok(py_expr_str) = col_any.cast::<PyExprStr>() {
            let session = THREAD_ACTIVE_SESSIONS
                .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "withColumn(expr) requires an active SparkSession",
                    )
                })?;
            let session_ref = session.bind(py).cast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            // Use a temp view so expr_string_to_polars can resolve column names against this DataFrame.
            session_ref
                .borrow()
                .inner
                .create_or_replace_temp_view("__withcol_expr_t", slf.inner.clone());
            let expr = robin_sparkless::sql::expr_string_to_polars(
                &py_expr_str.borrow().sql,
                &session_ref.borrow().inner,
                &slf.inner,
            )
            .map_err(to_py_err)?;
            session_ref
                .borrow()
                .inner
                .drop_temp_view("__withcol_expr_t");
            // Ignore boolean return; cleanup best-effort.
            let col = Column::from_expr(expr, None);
            let df = slf.inner.with_column(name, &col).map_err(|e| {
                let msg = e.to_string();
                if msg.contains("to_timestamp requires StringType") {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg)
                } else {
                    to_py_err(msg)
                }
            })?;
            df.schema_engine().map_err(|e| {
                let msg = e.to_string();
                if msg.contains("to_timestamp requires StringType") {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg)
                } else {
                    to_py_err(msg)
                }
            })?;
            return Ok(slf.wrap_with_column_derived(df, name, col.expr()));
        }

        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "withColumn expects a Column or expr() result",
        ))
    }

    #[pyo3(name = "withColumn")]
    fn with_column_camel(
        slf: PyRef<Self>,
        py: Python<'_>,
        name: &str,
        col: &Bound<'_, PyAny>,
    ) -> PyResult<PyDataFrame> {
        Self::with_column(slf, py, name, col)
    }

    #[pyo3(name = "withColumnRenamed")]
    fn with_column_renamed(&self, old_name: &str, new_name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .with_column_renamed(old_name, new_name)
            .map(|df| self.wrap_renamed_derived(df, old_name, new_name))
            .map_err(to_py_err)
    }

    #[pyo3(name = "__getitem__")]
    fn get_item(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        // PySpark: df["col"] -> Column; df[["a","b"]] -> DataFrame selecting those columns.
        // Sparkless parity: accept list/tuple of str for projection.
        if let Ok(name) = key.extract::<String>() {
            if name == "*" {
                let columns = self.inner.columns().map_err(to_py_err)?;
                return Py::new(py, PyStarColumns { columns })?.into_py_any(py);
            }
            let col = self
                .inner
                .column(&name)
                .map(|c| PyColumn { inner: c })
                .map_err(to_py_err)?;
            return col.into_py_any(py);
        }
        if let Ok(list) = key.cast::<PyList>() {
            let mut names: Vec<String> = Vec::with_capacity(list.len());
            for item in list.iter() {
                names.push(item.extract::<String>()?);
            }
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let df = self
                .inner
                .select(refs)
                .map(|df| self.wrap_derived(df))
                .map_err(to_py_err)?;
            return df.into_py_any(py);
        }
        if let Ok(tup) = key.cast::<PyTuple>() {
            let mut names: Vec<String> = Vec::with_capacity(tup.len());
            for item in tup.iter() {
                names.push(item.extract::<String>()?);
            }
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let df = self
                .inner
                .select(refs)
                .map(|df| self.wrap_derived(df))
                .map_err(to_py_err)?;
            return df.into_py_any(py);
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "__getitem__ expects a column name (str) or list/tuple of column names",
        ))
    }

    fn __getattr__(&self, name: &str) -> PyResult<PyColumn> {
        if name.starts_with('_') {
            return Err(PyErr::new::<pyo3::exceptions::PyAttributeError, _>(
                name.to_string(),
            ));
        }
        self.inner
            .column(name)
            .map(|c| PyColumn { inner: c })
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyAttributeError, _>(name.to_string()))
    }

    #[pyo3(signature = (n=20, truncate=true))]
    fn show(&self, py: Python<'_>, n: usize, truncate: bool) -> PyResult<()> {
        // truncate: PySpark parity (e.g. show(truncate=False)); display layer may use it later
        let _ = truncate;
        self.check_mixed_row_shape()?;
        let action = self.begin_action();
        let token = action.token.clone();
        let inner = self.inner.clone();
        py.detach(move || inner.show_with_cancellation(Some(n), &token))
            .map_err(to_py_err)
    }

    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        if let Some(ref cached) = self.schema_cache {
            return cached.clone_ref(py).into_py_any(py);
        }
        let schema = self.inner.schema_engine().map_err(to_py_err)?;
        let types_mod = PyModule::import(py, "sparkless.sql.types")?;
        let struct_type_cls = types_mod.getattr("StructType")?;
        let struct_field_cls = types_mod.getattr("StructField")?;

        fn dtype_to_py(
            py: Python<'_>,
            types_mod: &Bound<'_, PyModule>,
            dt: &DataType,
            path: &str,
            map_paths: &std::collections::HashSet<String>,
        ) -> PyResult<PyObject> {
            let mk0 = |name: &str| -> PyResult<PyObject> {
                types_mod.getattr(name)?.call0()?.into_py_any(py)
            };
            match dt {
                DataType::String => mk0("StringType"),
                DataType::Short => mk0("ShortType"),
                DataType::Integer => mk0("IntegerType"),
                DataType::Long => mk0("LongType"),
                DataType::Float => mk0("FloatType"),
                DataType::Double => mk0("DoubleType"),
                DataType::Decimal(precision, scale) => Ok(types_mod
                    .getattr("DecimalType")?
                    .call1((*precision, *scale))?
                    .into_py_any(py)?),
                DataType::Boolean => mk0("BooleanType"),
                DataType::Date => mk0("DateType"),
                DataType::Timestamp => mk0("TimestampType"),
                DataType::TimestampNtz => mk0("TimestampNTZType"),
                DataType::Binary => mk0("BinaryType"),
                DataType::Array(elem)
                    if map_paths.contains(path)
                        && matches!(elem.as_ref(), DataType::LogicalStruct(fields)
                            if fields.iter().any(|field| field.name == "key")
                                && fields.iter().any(|field| field.name == "value")) =>
                {
                    let DataType::LogicalStruct(fields) = elem.as_ref() else {
                        unreachable!()
                    };
                    let key = fields.iter().find(|field| field.name == "key").unwrap();
                    let value = fields.iter().find(|field| field.name == "value").unwrap();
                    let key_py = dtype_to_py(
                        py,
                        types_mod,
                        &key.data_type,
                        &format!("{path}{{key}}"),
                        map_paths,
                    )?;
                    let value_py = dtype_to_py(
                        py,
                        types_mod,
                        &value.data_type,
                        &format!("{path}{{value}}"),
                        map_paths,
                    )?;
                    Ok(types_mod
                        .getattr("MapType")?
                        .call1((key_py, value_py, true))?
                        .into_py_any(py)?)
                }
                DataType::Array(elem) => {
                    let elem_py =
                        dtype_to_py(py, types_mod, elem, &format!("{path}[]"), map_paths)?;
                    Ok(types_mod
                        .getattr("ArrayType")?
                        // PySpark: many array-returning functions (including split) use // containsNull=False for elementType=string. We default to False
                        // here so schema.jsonValue() matches PySpark parity for cases
                        // like string.split_limit (issue #1390).
                        .call1((elem_py, false))?
                        .into_py_any(py)?)
                }
                DataType::Map(k, v) => {
                    let k_py = dtype_to_py(py, types_mod, k, &format!("{path}{{key}}"), map_paths)?;
                    let v_py =
                        dtype_to_py(py, types_mod, v, &format!("{path}{{value}}"), map_paths)?;
                    Ok(types_mod
                        .getattr("MapType")?
                        .call1((k_py, v_py, true))?
                        .into_py_any(py)?)
                }
                DataType::LogicalStruct(fields) => {
                    let mut py_fields: Vec<PyObject> = Vec::with_capacity(fields.len());
                    for f in fields {
                        let dt_obj = dtype_to_py(
                            py,
                            types_mod,
                            &f.data_type,
                            &arrow_field_path(path, &f.name),
                            map_paths,
                        )?;
                        let sf = types_mod.getattr("StructField")?.call1((
                            f.name.clone(),
                            dt_obj,
                            f.nullable,
                            py.None(),
                        ))?;
                        py_fields.push(sf.into_py_any(py)?);
                    }
                    Ok(types_mod
                        .getattr("StructType")?
                        .call1((py_fields,))?
                        .into_py_any(py)?)
                }
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unsupported schema data type {dt:?}"
                ))),
            }
        }

        let mut py_fields: Vec<PyObject> = Vec::with_capacity(schema.logical_fields().len());
        for f in schema.logical_fields() {
            let dt_obj = dtype_to_py(
                py,
                &types_mod,
                &f.data_type,
                &arrow_field_path("", &f.name),
                &self.logical_map_paths,
            )?;
            let sf = struct_field_cls.call1((f.name.clone(), dt_obj, f.nullable, py.None()))?;
            py_fields.push(sf.into_py_any(py)?);
        }
        struct_type_cls.call1((py_fields,))?.into_py_any(py)
    }

    #[getter]
    fn _schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.schema(py)
    }

    /// Collect all rows as a list of Row objects (PySpark parity).
    ///
    /// **Contract:** Returns `list[Row]`. Each Row is tuple-like with attribute and name-based
    /// access; types are preserved (int, float, bool, str, None, date, datetime, list, dict).
    /// Use `row.asDict()` for a dict of column name -> value, or iterate for values in order.
    /// String-typed columns are best-effort coerced to int/float/bool when the value looks like
    /// one (e.g. coalesce() results or mixed-type array elements stringified by the engine).
    fn collect(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        self.check_mixed_row_shape()?;
        let action = self.begin_action();
        let token = action.token.clone();
        let materialization_token = token.clone();
        let inner = self.inner.clone();
        let array_struct_paths = self.arrow_array_struct_paths.clone();
        // Build a PySpark-like Row with schema attached. Use schema from collected result so
        // get_json_object etc. are string (PySpark parity #1146).
        let (output_names, rows, schema) = py
            .detach(move || {
                inner.collect_as_json_rows_with_names_and_array_struct_columns_with_cancellation(
                    &array_struct_paths,
                    &materialization_token,
                )
            })
            .map_err(to_py_err)?;
        let mut dtype_by_name: std::collections::HashMap<String, DataType> =
            std::collections::HashMap::with_capacity(schema.logical_fields().len());
        for f in schema.logical_fields() {
            dtype_by_name.insert(f.name.clone(), f.data_type.clone());
        }

        let types_mod = PyModule::import(py, "sparkless.sql.types")?;
        let row_cls = types_mod.getattr("Row")?;
        let struct_type_cls = types_mod.getattr("StructType")?;
        let struct_field_cls = types_mod.getattr("StructField")?;

        fn dtype_to_py(
            py: Python<'_>,
            types_mod: &Bound<'_, PyModule>,
            dt: &DataType,
        ) -> PyResult<PyObject> {
            let mk0 = |name: &str| -> PyResult<PyObject> {
                types_mod.getattr(name)?.call0()?.into_py_any(py)
            };
            match dt {
                DataType::String => mk0("StringType"),
                DataType::Short => mk0("ShortType"),
                DataType::Integer => mk0("IntegerType"),
                DataType::Long => mk0("LongType"),
                DataType::Float => mk0("FloatType"),
                DataType::Double => mk0("DoubleType"),
                DataType::Decimal(precision, scale) => Ok(types_mod
                    .getattr("DecimalType")?
                    .call1((*precision, *scale))?
                    .into_py_any(py)?),
                DataType::Boolean => mk0("BooleanType"),
                DataType::Date => mk0("DateType"),
                DataType::Timestamp => mk0("TimestampType"),
                DataType::TimestampNtz => mk0("TimestampNTZType"),
                DataType::Binary => mk0("BinaryType"),
                DataType::Array(elem) => {
                    let elem_py = dtype_to_py(py, types_mod, elem)?;
                    Ok(types_mod
                        .getattr("ArrayType")?
                        .call1((elem_py, true))?
                        .into_py_any(py)?)
                }
                DataType::Map(k, v) => {
                    let k_py = dtype_to_py(py, types_mod, k)?;
                    let v_py = dtype_to_py(py, types_mod, v)?;
                    Ok(types_mod
                        .getattr("MapType")?
                        .call1((k_py, v_py, true))?
                        .into_py_any(py)?)
                }
                DataType::LogicalStruct(fields) => {
                    let mut py_fields: Vec<PyObject> = Vec::with_capacity(fields.len());
                    for f in fields {
                        let dt_obj = dtype_to_py(py, types_mod, &f.data_type)?;
                        let sf = types_mod.getattr("StructField")?.call1((
                            f.name.clone(),
                            dt_obj,
                            f.nullable,
                            py.None(),
                        ))?;
                        py_fields.push(sf.into_py_any(py)?);
                    }
                    Ok(types_mod
                        .getattr("StructType")?
                        .call1((py_fields,))?
                        .into_py_any(py)?)
                }
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unsupported schema data type {dt:?}"
                ))),
            }
        }

        // Build StructType schema object once.
        let mut py_fields: Vec<PyObject> = Vec::with_capacity(schema.logical_fields().len());
        for f in schema.logical_fields() {
            let dt_obj = dtype_to_py(py, &types_mod, &f.data_type)?;
            let sf = struct_field_cls.call1((f.name.clone(), dt_obj, f.nullable, py.None()))?;
            py_fields.push(sf.into_py_any(py)?);
        }
        let py_schema = struct_type_cls.call1((py_fields,))?.into_py_any(py)?;

        let ym_interval_cols: std::collections::HashSet<String> = self
            .schema_cache
            .as_ref()
            .and_then(|cached| ym_interval_column_names_from_schema(py, cached.bind(py)).ok())
            .unwrap_or_default();
        let dt_interval_cols: std::collections::HashSet<String> = self
            .schema_cache
            .as_ref()
            .and_then(|cached| dt_interval_column_names_from_schema(py, cached.bind(py)).ok())
            .unwrap_or_default();
        let variant_cols: std::collections::HashSet<String> = self
            .schema_cache
            .as_ref()
            .and_then(|cached| variant_column_names_from_schema(py, cached.bind(py)).ok())
            .unwrap_or_default();
        let interval_legacy = interval_legacy_collect();
        let ym_interval_cls = types_mod.getattr("YearMonthInterval").ok();

        let datetime_mod = PyModule::import(py, "datetime")?;
        let datetime_cls = datetime_mod.getattr("datetime")?;
        let date_cls = datetime_mod.getattr("date")?;
        let timedelta_cls = datetime_mod.getattr("timedelta")?;

        // output_names and rows already obtained above with schema.
        let mut out = Vec::with_capacity(rows.len());
        for (row_index, row) in rows.into_iter().enumerate() {
            token.check_cancelled().map_err(to_py_err)?;
            let kwargs = PyDict::new(py);
            for name in &output_names {
                let v = row.get(name).unwrap_or(&JsonValue::Null);
                let py_v = collect_cell_py(
                    py,
                    name,
                    v,
                    &ym_interval_cols,
                    &dt_interval_cols,
                    &variant_cols,
                    interval_legacy,
                    ym_interval_cls.as_ref(),
                    &timedelta_cls,
                    &dtype_by_name,
                    &output_names,
                    &datetime_cls,
                    &date_cls,
                )?;
                kwargs.set_item(name, py_v)?;
            }
            let py_row = row_cls.call((), Some(&kwargs))?;
            py_row.setattr("_fields", output_names.clone())?;
            py_row.setattr("__fields__", output_names.clone())?;
            py_row.setattr("_schema", py_schema.clone_ref(py))?;
            // PySpark parity: Row has _data_dict for dict-like access in tests (e.g. "full_name" in result[0].__dict__["_data_dict"]).
            py_row.setattr("_data_dict", kwargs.clone())?;
            token.check_cancelled().map_err(to_py_err)?;
            out.push(py_row.into_py_any(py)?);
            if row_index % 32 == 31 {
                yield_to_python_control_threads(py);
                token.check_cancelled().map_err(to_py_err)?;
            }
        }
        yield_to_python_control_threads(py);
        token.check_cancelled().map_err(to_py_err)?;
        Ok(out)
    }

    /// Cancel active actions started through this DataFrame object.
    /// A count is returned so callers can tell whether any work was still active.
    fn cancel(&self) -> usize {
        self.active_actions.cancel_active()
    }

    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        self.check_mixed_row_shape()?;
        let action = self.begin_action();
        let token = action.token.clone();
        let inner = self.inner.clone();
        py.detach(move || inner.count_with_cancellation(&token))
            .map_err(to_py_err)
    }

    /// PySpark parity: len(df) returns row count.
    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        self.check_mixed_row_shape()?;
        let action = self.begin_action();
        let token = action.token.clone();
        let inner = self.inner.clone();
        py.detach(move || inner.count_with_cancellation(&token))
            .map_err(to_py_err)
    }

    /// PySpark parity: head() returns the first Row (or None if empty); head(n) returns a list of Row.
    #[pyo3(signature = (n=None))]
    fn head(&self, py: Python<'_>, n: Option<usize>) -> PyResult<PyObject> {
        let k = n.unwrap_or(1);
        let limited = self.inner.limit(k).map_err(to_py_err)?;
        let mut limited_py = self.wrap_derived(limited);
        limited_py.active_actions = self.active_actions.clone();
        let rows = limited_py.collect(py)?;
        match n {
            Some(_) => Ok(PyList::new(py, rows)?.into_py_any(py)?),
            None => Ok(rows
                .into_iter()
                .next()
                .unwrap_or_else(|| py.None().into_any())),
        }
    }

    #[getter]
    fn na(slf: PyRef<Self>) -> PyDataFrameNaFunctions {
        let py = slf.py();
        PyDataFrameNaFunctions {
            df: slf.into_py_any(py).unwrap(),
        }
    }

    fn alias(&self, name: &str) -> PyDataFrame {
        self.wrap_derived(self.inner.alias(name))
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        fn push_expr(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<robin_sparkless::Expr>,
        ) -> PyResult<()> {
            if let Ok(c) = item.cast::<PyColumn>() {
                out.push(c.borrow().inner.clone().into_expr());
                return Ok(());
            }
            if let Ok(list) = item.cast::<PyList>() {
                for sub in list.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.cast::<PyTuple>() {
                for sub in tup.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "agg() expects Column expressions",
            ))
        }

        let mut rust_exprs: Vec<robin_sparkless::Expr> = Vec::new();
        for item in exprs.iter() {
            push_expr(&item, &mut rust_exprs)?;
        }
        self.inner
            .agg(rust_exprs.clone())
            .map(|df| self.wrap_aggregate_derived(df, &rust_exprs, 0))
            .map_err(to_py_err)
    }

    fn cache(&self) -> PyResult<PyDataFrame> {
        self.inner
            .cache()
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*cols))]
    fn rollup(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyCubeRollupData> {
        // PySpark: rollup(("a", "b")) is invalid – a bare tuple of column-name
        // strings is not accepted. It raises PySparkTypeError[NOT_COLUMN_OR_STR].
        if cols.len() == 1 {
            let item0 = cols.get_item(0)?;
            if let Ok(tup) = item0.cast::<PyTuple>() {
                let mut all_strings = true;
                for sub in tup.iter() {
                    if sub.extract::<String>().is_err() {
                        all_strings = false;
                        break;
                    }
                }
                if all_strings {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Argument `col` should be a Column or str (NOT_COLUMN_OR_STR: tuple is not allowed; use a list of columns instead)",
                    ));
                }
            }
        }
        let names = py_tuple_or_single_to_vec_string(cols)?;
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .rollup(refs)
            .map(|cr| PyCubeRollupData {
                inner: cr,
                source: self.wrap_derived(self.inner.clone()),
                grouping_column_count: names.len(),
            })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*cols, ascending=None))]
    fn sort(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.order_by_camel(cols, ascending)
    }

    #[pyo3(name = "groupby", signature = (*cols))]
    fn groupby(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyGroupedData> {
        self.group_by_camel(cols)
    }

    #[getter]
    fn rdd(slf: PyRef<Self>) -> PyRDD {
        let inner = slf.inner.clone();
        let source_df = Py::from(slf);
        PyRDD {
            source_df: Some(source_df),
            inner: Some(inner),
            elements: None,
        }
    }

    #[getter]
    fn storage(&self) -> PyResult<PyDataFrame> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "storage is not yet implemented in robin-sparkless",
        ))
    }

    #[pyo3(signature = (*cols))]
    fn cube(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyCubeRollupData> {
        // PySpark: cube(("a", "b")) is invalid – a bare tuple of column-name
        // strings is not accepted. It raises PySparkTypeError[NOT_COLUMN_OR_STR].
        if cols.len() == 1 {
            let item0 = cols.get_item(0)?;
            if let Ok(tup) = item0.cast::<PyTuple>() {
                let mut all_strings = true;
                for sub in tup.iter() {
                    if sub.extract::<String>().is_err() {
                        all_strings = false;
                        break;
                    }
                }
                if all_strings {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Argument `col` should be a Column or str (NOT_COLUMN_OR_STR: tuple is not allowed; use a list of columns instead)",
                    ));
                }
            }
        }
        let names = py_tuple_or_single_to_vec_string(cols)?;
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .cube(refs)
            .map(|cr| PyCubeRollupData {
                inner: cr,
                source: self.wrap_derived(self.inner.clone()),
                grouping_column_count: names.len(),
            })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (column_names))]
    fn group_by(&self, column_names: &Bound<'_, PyAny>) -> PyResult<PyGroupedData> {
        let names = py_cols_to_vec(column_names)?;
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let grouping_column_count = names.len();
        self.inner
            .group_by(refs)
            .map(|gd| PyGroupedData {
                inner: gd,
                source: self.wrap_derived(self.inner.clone()),
                grouping_column_count,
            })
            .map_err(to_py_err)
    }

    #[pyo3(name = "groupBy", signature = (*cols))]
    fn group_by_camel(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyGroupedData> {
        // PySpark: groupBy(("a", "b")) is invalid – a bare tuple of column-name
        // strings is not accepted and raises NOT_COLUMN_OR_STR. Lists (["a","b"])
        // and tuples/lists of Columns are allowed.
        if cols.len() == 1 {
            let item0 = cols.get_item(0)?;
            if let Ok(tup) = item0.cast::<PyTuple>() {
                let mut all_strings = true;
                for sub in tup.iter() {
                    if sub.cast::<PyColumn>().is_ok() || sub.cast::<PyExprStr>().is_ok() {
                        all_strings = false;
                        break;
                    }
                    if sub.extract::<String>().is_err() {
                        all_strings = false;
                        break;
                    }
                }
                if all_strings {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Argument `col` should be a Column or str (NOT_COLUMN_OR_STR: tuple is not allowed; use a list of columns instead)",
                    ));
                }
            }
        }
        let mut specs: Vec<GroupBySpec> = Vec::with_capacity(cols.len());
        for item in cols.iter() {
            for spec in py_group_by_specs(&item)? {
                specs.push(spec);
            }
        }
        let grouping_column_count = specs.len();
        self.inner
            .group_by_specs(specs)
            .map(|gd| PyGroupedData {
                inner: gd,
                source: self.wrap_derived(self.inner.clone()),
                grouping_column_count,
            })
            .map_err(to_py_err)
    }

    fn order_by_names(
        &self,
        column_names: Vec<String>,
        ascending: Vec<bool>,
    ) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let mut asc = ascending;
        while asc.len() < refs.len() {
            asc.push(true);
        }
        asc.truncate(refs.len());
        self.inner
            .order_by(refs, asc)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (cols, ascending=None))]
    fn do_order_by(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        // Core implementation is split out so we can optionally apply
        // PySpark-parity fallbacks when column resolution fails.
        self.do_order_by_impl(cols, ascending)
    }

    #[allow(clippy::too_many_lines)]
    #[pyo3(signature = (cols, ascending=None))]
    fn do_order_by_impl(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        // Flatten: single item may be a list of columns.
        // IMPORTANT: We intentionally do NOT flatten tuples here.
        // In PySpark, a bare tuple passed to sort/orderBy (e.g. df.sort(("a", "b")))
        // raises PySparkTypeError[NOT_COLUMN_OR_STR] instead of being treated like a list.
        // To match that behavior, tuples are treated as invalid inputs below rather than
        // silently flattened.
        let mut flat: Vec<Bound<'_, PyAny>> = Vec::new();
        for item in cols.iter() {
            if let Ok(list) = item.cast::<PyList>() {
                for sub in list.iter() {
                    flat.push(sub.clone());
                }
            } else if item.cast::<PyTuple>().is_ok() {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Argument `col` should be a Column or str (NOT_COLUMN_OR_STR: tuple is not allowed; use a list of columns instead)",
                ));
            } else {
                flat.push(item.clone());
            }
        }

        // PySpark parity: ascending can be None (all True), a single bool, or a list of bools.
        let asc_vec = parse_order_by_ascending(ascending, flat.len())?;

        // Try to build sort orders from Column/SortOrder/string (for order_by_exprs).
        let mut sort_orders: Vec<SortOrder> = Vec::with_capacity(flat.len());
        let mut names_only: Vec<String> = Vec::with_capacity(flat.len());
        let mut all_strings = true;
        for (i, item) in flat.iter().enumerate() {
            let asc = asc_vec.get(i).copied().unwrap_or(true);
            if let Ok(ps) = item.cast::<PySortOrder>() {
                // Explicit SortOrder (e.g. col("x").desc()), use as-is.
                sort_orders.push(ps.borrow().inner.clone());
                all_strings = false;
            } else if let Ok(pc) = item.cast::<PyColumn>() {
                // Bare Column: apply ascending/descending from asc_vec.
                let so = if asc {
                    pc.borrow().inner.asc_nulls_last()
                } else {
                    pc.borrow().inner.desc()
                };
                sort_orders.push(so);
                all_strings = false;
            } else if let (Ok(name_attr), Ok(asc_attr)) =
                (item.getattr("name"), item.getattr("ascending"))
            {
                // Sort-key object from sparkless.sql.functions.desc/asc: has .name and .ascending.
                let name: String = name_attr.extract()?;
                let ascending_flag: bool = asc_attr.extract()?;
                let col = Column::new(name.clone());
                let so = if ascending_flag {
                    functions::asc(&col)
                } else {
                    functions::desc(&col)
                };
                sort_orders.push(so);
                all_strings = false;
            } else if let Ok(s) = item.extract::<String>() {
                // Plain column name string.
                sort_orders.push(asc_from_name(&s));
                names_only.push(s);
            } else {
                // Fallback: give up on mixed types; try pure string path below.
                sort_orders.clear();
                names_only.clear();
                all_strings = false;
                break;
            }
        }

        if sort_orders.len() == flat.len() {
            if all_strings {
                return self.order_by_names(names_only, asc_vec);
            }
            return self
                .inner
                .order_by_exprs(sort_orders)
                .map(|df| self.wrap_derived(df))
                .map_err(to_py_err);
        }

        // Fallback: all column names as str (e.g. list of strings from a different structure)
        let mut names: Vec<String> = Vec::with_capacity(flat.len());
        for item in &flat {
            if let Ok(s) = item.extract::<String>() {
                names.push(s);
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "orderBy() expects column names as str or Column/SortOrder expressions",
                ));
            }
        }

        self.order_by_names(names, asc_vec)
    }

    /// order_by_exprs([col("a").asc(), col("b").desc_nulls_last()]) - order by SortOrder expressions.
    fn order_by_exprs(&self, exprs: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let list = exprs.cast::<PyList>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "order_by_exprs expects a list of Column or SortOrder",
            )
        })?;
        let mut sort_orders: Vec<SortOrder> = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(ps) = item.cast::<PySortOrder>() {
                sort_orders.push(ps.borrow().inner.clone());
            } else if let Ok(pc) = item.cast::<PyColumn>() {
                sort_orders.push(pc.borrow().inner.asc());
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "order_by_exprs list elements must be Column or SortOrder (e.g. col('x').asc())",
                ));
            }
        }
        self.inner
            .order_by_exprs(sort_orders)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*cols, ascending=None))]
    fn order_by(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.do_order_by(cols, ascending)
    }

    #[pyo3(name = "orderBy", signature = (*cols, ascending=None))]
    fn order_by_camel(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.do_order_by(cols, ascending)
    }

    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        self.inner
            .limit(n)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*columns))]
    fn drop(&self, columns: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let specs = py_tuple_or_single_to_drop_specs(columns)?;
        self.inner
            .drop_specs(specs)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (subset=None))]
    fn distinct(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        self.inner
            .distinct(sub)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(name = "dropDuplicates", signature = (subset=None, *cols))]
    fn drop_duplicates(
        &self,
        subset: Option<Vec<String>>,
        cols: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        let names = if !cols.is_empty() { Some(cols) } else { subset };
        self.distinct(names)
    }

    #[pyo3(name = "drop_duplicates", signature = (subset=None, *cols))]
    fn drop_duplicates_snake(
        &self,
        subset: Option<Vec<String>>,
        cols: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        self.drop_duplicates(subset, cols)
    }

    #[pyo3(signature = (other, on=None, how="inner", left_on=None, right_on=None))]
    fn join(
        &self,
        other: &PyDataFrame,
        on: Option<&Bound<'_, PyAny>>,
        how: &str,
        left_on: Option<&Bound<'_, PyAny>>,
        right_on: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let how_lower = how.to_lowercase();
        if on.is_some() && (left_on.is_some() || right_on.is_some()) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "join() cannot combine 'on' with 'left_on' or 'right_on'",
            ));
        }
        if left_on.is_some() != right_on.is_some() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "join() requires both 'left_on' and 'right_on' when using separate join keys",
            ));
        }
        if how_lower == "cross" {
            return self
                .inner
                .cross_join(&other.inner)
                .map(|df| self.wrap_join_derived(other, df))
                .map_err(to_py_err);
        }
        let join_type = match how_lower.as_str() {
            "inner" => JoinType::Inner,
            "left" | "left_outer" => JoinType::Left,
            "right" | "right_outer" => JoinType::Right,
            "outer" | "full" | "full_outer" | "fullouter" => JoinType::Outer,
            "semi" | "left_semi" | "leftsemi" => JoinType::LeftSemi,
            "anti" | "left_anti" | "leftanti" => JoinType::LeftAnti,
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unsupported join type: {other}. Supported: inner, left, right, outer, semi, anti, cross."
                )));
            }
        };
        let use_left_right = left_on.is_some() && right_on.is_some();
        if use_left_right {
            let left_vec = py_join_on_to_vec(left_on.unwrap())?;
            let right_vec = py_join_on_to_vec(right_on.unwrap())?;
            let left_refs: Vec<&str> = left_vec.iter().map(|s| s.as_str()).collect();
            let right_refs: Vec<&str> = right_vec.iter().map(|s| s.as_str()).collect();
            self.inner
                .join_with_keys(
                    &other.inner,
                    left_refs,
                    right_refs,
                    join_type,
                    false, // left_on/right_on: do not mark join keys ambiguous
                )
                .map(|df| self.wrap_join_derived(other, df))
                .map_err(to_py_err)
        } else if let Some(on_arg) = on {
            // Try to use normal join when on is simple column name(s) (PySpark parity: one key column in result).
            if let Ok(on_vec) = py_join_on_to_vec(on_arg) {
                let use_normal_join =
                    !on_vec.is_empty() && !on_vec.iter().any(|n| n == "<expr>" || n.is_empty());
                if use_normal_join {
                    let on_refs: Vec<&str> = on_vec.iter().map(|s| s.as_str()).collect();
                    return self
                        .inner
                        .join(&other.inner, on_refs, join_type)
                        .map(|df| self.wrap_join_derived(other, df))
                        .map_err(to_py_err);
                }
            }
            // Complex expression (e.g. left.dept_id == right.dept_id): try key-based join first (#1049, #1148).
            // PySpark also accepts a list of boolean conditions ANDed together (#1554).
            let join_condition_expr = if let Ok(condition) = on_arg.cast::<PyColumn>() {
                Some(condition.borrow().inner.clone().into_expr())
            } else {
                try_combine_join_condition_list(on_arg)?
            };
            if let Some(expr) = join_condition_expr {
                let pairs = try_extract_join_eq_columns_all(&expr);
                if !pairs.is_empty() {
                    let left_cols: std::collections::HashSet<String> = self
                        .inner
                        .columns()
                        .map_err(to_py_err)?
                        .into_iter()
                        .collect();
                    let right_cols: std::collections::HashSet<String> = other
                        .inner
                        .columns()
                        .map_err(to_py_err)?
                        .into_iter()
                        .collect();
                    let mut left_refs: Vec<String> = Vec::with_capacity(pairs.len());
                    let mut right_refs: Vec<String> = Vec::with_capacity(pairs.len());
                    for (key_a, key_b) in &pairs {
                        let (left_key, right_key) = resolve_join_keys_with_aliases(
                            key_a,
                            key_b,
                            &left_cols,
                            &right_cols,
                            self.inner.get_alias().as_deref(),
                            other.inner.get_alias().as_deref(),
                        );
                        if let (Some(lk), Some(rk)) = (left_key, right_key) {
                            left_refs.push(lk);
                            right_refs.push(rk);
                        }
                    }
                    if left_refs.len() == pairs.len() {
                        // Preserve resolved key names as owned Strings for later filtering decisions.
                        let left_keys: Vec<String> = left_refs.clone();
                        let right_keys: Vec<String> = right_refs.clone();
                        let left_refs: Vec<&str> = left_refs.iter().map(|s| s.as_str()).collect();
                        let right_refs: Vec<&str> = right_refs.iter().map(|s| s.as_str()).collect();
                        let only_key_equalities = expr_contains_only_join_key_equalities(&expr);

                        // When joining on equal same-named keys (e.g. emp.dept_id == dept.dept_id),
                        // implement LEFT/RIGHT joins via a FULL OUTER join plus a filter so that:
                        // - LEFT keeps all left rows, with right-only rows dropped.
                        // - RIGHT keeps all right rows, with left-only rows dropped.
                        // This matches the parity fixtures in tests/parity/dataframe/test_join.py,
                        // which expose both key columns as (dept_id, dept_id_right).
                        let same_named_keys = left_keys.len() == right_keys.len()
                            && left_keys
                                .iter()
                                .zip(right_keys.iter())
                                .all(|(l, r)| l.eq_ignore_ascii_case(r));

                        let engine_join_type = if only_key_equalities
                            && same_named_keys
                            && matches!(join_type, JoinType::Left | JoinType::Right)
                        {
                            JoinType::Outer
                        } else {
                            join_type
                        };

                        let mut joined = self
                            .inner
                            .join_with_keys(
                                &other.inner,
                                left_refs,
                                right_refs,
                                engine_join_type,
                                only_key_equalities,
                            )
                            .map_err(to_py_err)?;

                        // When the expression is only key equalities, the join itself already
                        // enforces the predicate; applying filter(expr) would be redundant and,
                        // for LEFT/RIGHT/OUTER joins, would drop unmatched rows and break join
                        // semantics (#1242). For compound predicates (e.g. key equality AND an
                        // additional filter like amount > 30, issue #380), we must reapply the
                        // full predicate after the key-based join.
                        let expr_has_only_key_equalities =
                            expr_contains_only_join_key_equalities(&expr);
                        let should_apply_filter = !expr_has_only_key_equalities;
                        if should_apply_filter {
                            joined = joined.filter(expr).map_err(to_py_err)?;
                        }

                        // For LEFT / RIGHT joins implemented via FULL OUTER on same-named keys,
                        // drop rows from the non-preserved side only (keep left rows with null keys).
                        if only_key_equalities && same_named_keys {
                            use robin_sparkless::functions;
                            match join_type {
                                JoinType::Left => {
                                    let cond = left_keys.iter().zip(right_keys.iter()).fold(
                                        None::<robin_sparkless::Expr>,
                                        |acc, (k, _)| {
                                            let right_col_name = format!("{}_right", k.as_str());
                                            let left_nn = functions::col(k.as_str()).is_not_null();
                                            let right_null =
                                                functions::col(right_col_name.as_str()).is_null();
                                            let keep = left_nn.or_(&right_null);
                                            let c = keep.into_expr();
                                            Some(match acc {
                                                None => c,
                                                Some(prev) => prev.and(c),
                                            })
                                        },
                                    );
                                    if let Some(cond) = cond {
                                        joined = joined.filter(cond).map_err(to_py_err)?;
                                    }
                                }
                                JoinType::Right => {
                                    let cond = left_keys.iter().zip(right_keys.iter()).fold(
                                        None::<robin_sparkless::Expr>,
                                        |acc, (k, _)| {
                                            let right_col_name = format!("{}_right", k.as_str());
                                            let right_nn = functions::col(right_col_name.as_str())
                                                .is_not_null();
                                            let left_null = functions::col(k.as_str()).is_null();
                                            let keep = right_nn.or_(&left_null);
                                            let c = keep.into_expr();
                                            Some(match acc {
                                                None => c,
                                                Some(prev) => prev.and(c),
                                            })
                                        },
                                    );
                                    if let Some(cond) = cond {
                                        joined = joined.filter(cond).map_err(to_py_err)?;
                                    }
                                }
                                _ => {}
                            }
                        }

                        return Ok(self.wrap_join_derived(other, joined));
                    }
                }
                // Non-eq expression: cross + filter (ambiguous duplicate column names may apply).
                self.inner
                    .check_cross_join_budget(&other.inner)
                    .map_err(to_py_err)?;
                let crossed = self.inner.cross_join(&other.inner).map_err(to_py_err)?;
                let filtered = crossed.filter(expr).map_err(to_py_err)?;
                // For left/outer joins, include unmatched left rows with nulls on right columns (best-effort PySpark parity).
                if matches!(join_type, JoinType::Left | JoinType::Outer) {
                    // Columns coming from left side
                    let left_cols = self.inner.columns().map_err(to_py_err)?;
                    let left_refs: Vec<&str> = left_cols.iter().map(|s| s.as_str()).collect();
                    let filtered_left = filtered.select(left_refs).map_err(to_py_err)?;
                    // Unmatched left rows (set difference by all left columns)
                    let unmatched_left = self.inner.subtract(&filtered_left).map_err(to_py_err)?;
                    // Add null right columns to unmatched_left so schema matches filtered.
                    let all_cols = filtered.columns().map_err(to_py_err)?;
                    let mut augmented = unmatched_left;
                    for col_name in &all_cols {
                        // Skip columns that already exist on the left side.
                        if left_cols.iter().any(|c| c.as_str() == col_name.as_str()) {
                            continue;
                        }
                        let dtype_opt =
                            filtered
                                .get_column_dtype(col_name.as_str())
                                .ok_or_else(|| {
                                    to_py_err(format!(
                                        "join: failed to get dtype for column '{}'",
                                        col_name
                                    ))
                                })?;
                        // Heuristic mapping from engine dtype to PySpark type string.
                        let dtype_str = dtype_opt.to_string().to_lowercase();
                        let null_col = if dtype_str.contains("int") {
                            functions::lit_null("bigint").map_err(to_py_err)?
                        } else if dtype_str.contains("string") {
                            functions::lit_null("string").map_err(to_py_err)?
                        } else {
                            functions::lit_null_untyped()
                        };
                        augmented = augmented
                            .with_column(col_name.as_str(), &null_col)
                            .map_err(to_py_err)?;
                    }
                    // Reorder columns on augmented to match filtered, then union (no type promotion).
                    let all_refs: Vec<&str> = all_cols.iter().map(|s| s.as_str()).collect();
                    let augmented = augmented.select(all_refs).map_err(to_py_err)?;
                    let all = filtered.union(&augmented).map_err(to_py_err)?;
                    return Ok(self.wrap_join_derived(other, all));
                }
                return Ok(self.wrap_join_derived(other, filtered));
            }
            // Column-name list/tuple (no complex expressions): join on those keys.
            let on_vec = py_join_on_to_vec(on_arg)?;
            let on_refs: Vec<&str> = on_vec.iter().map(|s| s.as_str()).collect();
            self.inner
                .join(&other.inner, on_refs, join_type)
                .map(|df| self.wrap_join_derived(other, df))
                .map_err(to_py_err)
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "join() requires either on= or (left_on= and right_on=)",
            ))
        }
    }

    #[pyo3(name = "crossJoin")]
    fn cross_join(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .cross_join(&other.inner)
            .map(|df| self.wrap_join_derived(other, df))
            .map_err(to_py_err)
    }

    fn intersect(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .intersect(&other.inner)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(name = "exceptAll")]
    fn except_all(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .except_all(&other.inner)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn union(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let other_inner = py_any_to_dataframe_inner(other)?;
        let other_wrapper = other.cast::<PyDataFrame>().ok();
        if let Some(other_wrapper) = other_wrapper {
            self.validate_union_metadata(&other_wrapper.borrow(), false)?;
        }
        let df = self.inner.union(&other_inner).map_err(to_py_err)?;
        Ok(match other_wrapper {
            Some(other) => self.wrap_union_derived(Some(&other.borrow()), df, false),
            None => self.wrap_union_derived(None, df, false),
        })
    }

    fn union_all(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let other_inner = py_any_to_dataframe_inner(other)?;
        let other_wrapper = other.cast::<PyDataFrame>().ok();
        if let Some(other_wrapper) = other_wrapper {
            self.validate_union_metadata(&other_wrapper.borrow(), false)?;
        }
        let df = self.inner.union_all(&other_inner).map_err(to_py_err)?;
        Ok(match other_wrapper {
            Some(other) => self.wrap_union_derived(Some(&other.borrow()), df, false),
            None => self.wrap_union_derived(None, df, false),
        })
    }

    #[pyo3(name = "unionAll")]
    fn union_all_camel(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        self.union_all(other)
    }

    #[pyo3(name = "unionByName", signature = (other, **kwargs))]
    fn union_by_name(
        &self,
        other: &Bound<'_, PyAny>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyDataFrame> {
        let allow = extract_allow_missing_columns(kwargs)?;
        let other_inner = py_any_to_dataframe_inner(other)?;
        let other_wrapper = other.cast::<PyDataFrame>().ok();
        if let Some(other_wrapper) = other_wrapper {
            self.validate_union_metadata(&other_wrapper.borrow(), true)?;
        }
        let df = self
            .inner
            .union_by_name(&other_inner, allow)
            .map_err(to_py_err)?;
        Ok(match other_wrapper {
            Some(other) => self.wrap_union_derived(Some(&other.borrow()), df, true),
            None => self.wrap_union_derived(None, df, true),
        })
    }

    #[pyo3(name = "selectExpr", signature = (*exprs))]
    fn select_expr(&self, py: Python<'_>, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut exprs_vec: Vec<String> = Vec::with_capacity(exprs.len());
        for item in exprs.iter() {
            exprs_vec.push(item.extract::<String>()?);
        }
        // Prefer SQL-based parsing when session is available (e.g. "upper(Name) as u").
        let session_opt =
            THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
        if let Some(session) = session_opt {
            if let Ok(session_ref) = session.bind(py).cast::<PySparkSession>() {
                if let Ok(df) = self
                    .inner
                    .select_expr_with_session(&session_ref.borrow().inner, &exprs_vec)
                {
                    return Ok(self.wrap_select_expr_derived(df, &exprs_vec));
                }
            }
        }
        self.inner
            .select_expr(&exprs_vec)
            .map(|df| self.wrap_select_expr_derived(df, &exprs_vec))
            .map_err(to_py_err)
    }

    #[pyo3(name = "createOrReplaceTempView")]
    fn create_or_replace_temp_view_camel(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = require_active_session(py)?;
        let session_ref = session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        session_ref
            .borrow()
            .inner
            .create_or_replace_temp_view(name, self.inner.clone());
        session_ref.borrow().remember_delta_temp_view(name, self);
        Ok(())
    }

    #[pyo3(name = "createOrReplaceGlobalTempView")]
    fn create_or_replace_global_temp_view_camel(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = THREAD_ACTIVE_SESSIONS
            .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
            .ok_or_else(|| to_py_err("No active SparkSession for createOrReplaceGlobalTempView"))?;
        let session_ref = session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        session_ref
            .borrow()
            .inner
            .create_or_replace_global_temp_view(name, self.inner.clone());
        session_ref
            .borrow()
            .remember_delta_temp_view(&format!("global_temp.{name}"), self);
        Ok(())
    }

    #[pyo3(name = "dropna", signature = (subset=None, how=None, thresh=None))]
    fn dropna(
        &self,
        subset: Option<Vec<String>>,
        how: Option<&str>,
        thresh: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let how_str = how.unwrap_or("any");
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        self.inner
            .dropna(sub, how_str, thresh)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (value, subset=None))]
    fn fillna(
        &self,
        value: &Bound<'_, PyAny>,
        subset: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        // PySpark: fillna(scalar) or fillna({col: value, ...}); subset only with scalar.
        if let Ok(dict) = value.cast::<PyDict>() {
            let mut df = self.inner.clone();
            for item in dict.iter() {
                let col_name: String = item.0.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("fillna dict keys must be str")
                })?;
                let val_expr = py_any_to_column(&item.1)?.into_expr();
                df = df
                    .fillna(val_expr, Some(vec![col_name.as_str()]))
                    .map_err(to_py_err)?;
            }
            return Ok(self.wrap_derived(df));
        }
        let value_expr = py_any_to_column(value)?.into_expr();
        let kind = classify_fill_value_kind(value);
        let sub_vec = normalize_subset(subset)?;
        // Candidate columns: explicit subset or all columns when subset is None.
        let candidates: Vec<String> = match sub_vec {
            Some(cols) => cols,
            None => self.inner.columns().map_err(to_py_err)?,
        };
        let mut effective: Vec<&str> = Vec::new();
        for name in &candidates {
            match self.inner.get_column_data_type(name) {
                Some(dt) => {
                    if is_fill_compatible_with_schema_dtype(&dt, &kind) {
                        effective.push(name.as_str());
                    }
                }
                None => {
                    // Non-existent column: keep so engine raises ColumnNotFoundException as before.
                    effective.push(name.as_str());
                }
            }
        }
        let sub: Option<Vec<&str>> = Some(effective);
        self.inner
            .fillna(value_expr, sub)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    /// Replace value in columns. PySpark df.replace(to_replace, value=None, subset=...).
    /// Uses *values and **kwargs so we can distinguish "value not provided" from "value is Python None",
    /// and accept value when passed as keyword (e.g. from Python na.replace(to_replace, value=value, subset=...)).
    #[pyo3(signature = (to_replace, *values, subset=None, **kwargs))]
    fn replace(
        &self,
        to_replace: &Bound<'_, PyAny>,
        values: &Bound<'_, PyTuple>,
        subset: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyDataFrame> {
        let sub_vec = normalize_subset(subset)?;
        let sub: Option<Vec<&str>> = sub_vec
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());

        let value_opt: Option<Bound<'_, PyAny>> = if values.len() >= 1 {
            Some(values.get_item(0)?)
        } else if let Some(kw) = kwargs {
            kw.get_item("value")?
        } else {
            None
        };
        let value_provided = value_opt.is_some();

        if !value_provided {
            if let Ok(dict) = to_replace.cast::<PyDict>() {
                let mut current = self.inner.clone();
                for (k, v) in dict.iter() {
                    let old_expr = py_any_to_column(&k)?.into_expr();
                    let new_expr = py_any_to_column(&v)?.into_expr();
                    current = current
                        .na()
                        .replace(old_expr, new_expr, sub.as_ref().map(|v| v.to_vec()))
                        .map_err(to_py_err)?;
                }
                return Ok(self.wrap_derived(current));
            }
            return Err(to_py_err("value is required when to_replace is not a dict"));
        }

        let value = value_opt.as_ref().unwrap();
        // PySpark: na.replace does not accept None as to_replace; use fillna for nulls (#1210).
        if to_replace.is_none() {
            return Err(to_py_err(
                "na.replace does not accept None as to_replace; use fillna for nulls",
            ));
        }
        // PySpark: replace("a", None, subset=...) replaces "a" with null.
        if value.is_none() {
            let null_expr = robin_sparkless::functions::lit_null_untyped().into_expr();
            let old_expr = py_any_to_column(to_replace)?.into_expr();
            return self
                .inner
                .na()
                .replace(old_expr, null_expr, sub.as_ref().map(|v| v.to_vec()))
                .map(|df| self.wrap_derived(df))
                .map_err(to_py_err);
        }
        let old_expr = py_any_to_column(to_replace)?.into_expr();
        let new_expr = py_any_to_column(value)?.into_expr();
        self.inner
            .na()
            .replace(old_expr, new_expr, sub.as_ref().map(|v| v.to_vec()))
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    /// Return the first row as a Row, or None if empty (Phase 7.8 / PySpark first() semantics).
    fn first(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let one = self.inner.first().map_err(to_py_err)?;
        let mut wrapper = self.wrap_derived(one);
        wrapper.active_actions = self.active_actions.clone();
        let rows = wrapper.collect(py)?;
        Ok(rows.into_iter().next())
    }

    fn describe(&self) -> PyResult<PyDataFrame> {
        self.inner
            .describe()
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    /// PySpark alias for describe().
    fn summary(&self) -> PyResult<PyDataFrame> {
        self.describe()
    }

    /// PySpark: explain() prints to stdout and returns None (PySpark parity).
    #[pyo3(signature = (extended=None))]
    fn explain(&self, extended: Option<bool>) -> PyResult<Option<String>> {
        let _ = extended; // accepted for API parity; not yet used
        let plan = self.inner.explain();
        println!("{}", plan);
        Ok(None) // PySpark parity: explain() returns None
    }

    fn print_schema(&self) -> PyResult<String> {
        self.inner.print_schema().map_err(to_py_err)
    }

    #[pyo3(name = "printSchema")]
    fn print_schema_camel(&self) -> PyResult<String> {
        self.print_schema()
    }

    fn persist(&self) -> PyResult<PyDataFrame> {
        self.inner
            .persist()
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn subtract(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .subtract(&other.inner)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(name = "sampleBy", signature = (col, fractions, seed=None))]
    fn sample_by(
        &self,
        _py: Python<'_>,
        col: &str,
        fractions: &Bound<'_, PyAny>,
        seed: Option<u64>,
    ) -> PyResult<PyDataFrame> {
        let dict = fractions.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "sampleBy fractions must be a dict of {value: fraction}",
            )
        })?;
        let mut fracs: Vec<(robin_sparkless::Expr, f64)> = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let frac = v.extract::<f64>()?;
            let lit_col = py_any_to_column(&k)?;
            fracs.push((lit_col.into_expr(), frac));
        }
        self.inner
            .sample_by(col, &fracs, seed)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn crosstab(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        self.inner
            .crosstab(col1, col2)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(name = "freqItems", signature = (columns, support=0.01))]
    fn freq_items(&self, columns: Vec<String>, support: f64) -> PyResult<PyDataFrame> {
        let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.inner
            .freq_items(&cols, support)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn melt(&self, id_vars: Vec<String>, value_vars: Vec<String>) -> PyResult<PyDataFrame> {
        let ids: Vec<&str> = id_vars.iter().map(|s| s.as_str()).collect();
        let vals: Vec<&str> = value_vars.iter().map(|s| s.as_str()).collect();
        self.inner
            .melt(&ids, &vals)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (ids, values, *, variableColumnName=None, valueColumnName=None))]
    fn unpivot(
        &self,
        ids: Vec<String>,
        values: Vec<String>,
        variableColumnName: Option<String>,
        valueColumnName: Option<String>,
    ) -> PyResult<PyDataFrame> {
        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        let val_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        let mut df = self
            .inner
            .unpivot(&id_refs, &val_refs)
            .map(|df| self.wrap_derived(df))
            .map_err(to_py_err)?;
        if let Some(ref name) = variableColumnName {
            df = df.with_column_renamed("variable", name)?;
        }
        if let Some(ref name) = valueColumnName {
            df = df.with_column_renamed("value", name)?;
        }
        Ok(df)
    }

    #[pyo3(name = "flatMap")]
    fn flat_map_impl(&self, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let rows = self.collect(py)?;
        let mut out_rows: Vec<PyObject> = Vec::new();
        for row in rows {
            let row_bound = row.bind(py);
            let result = func.call1((row_bound,))?;
            for item in result.try_iter()? {
                let item: pyo3::Bound<'_, PyAny> = item?;
                out_rows.push(item.into_py_any(py)?);
            }
        }
        let session = THREAD_ACTIVE_SESSIONS
            .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
            .ok_or_else(|| to_py_err("No active SparkSession for flatMap"))?;
        let session_ref = session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let list = PyList::new(py, out_rows)?;
        let (rows_data, schema, schema_was_inferred, _mixed_row_kinds) =
            python_data_and_schema(py, list.as_ref(), None, false, None)?;
        let df = session_ref
            .borrow()
            .inner
            .create_dataframe_from_rows(rows_data, schema, false, schema_was_inferred)
            .map_err(to_py_err)?;
        Ok(self.wrap_derived(df))
    }

    #[pyo3(name = "flat_map")]
    fn flat_map(&self, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        self.flat_map_impl(py, func)
    }

    fn with_columns_renamed(&self, cols_map: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let dict = cols_map.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "withColumnsRenamed expects a dict of {old_name: new_name}",
            )
        })?;
        let mut renames: Vec<(String, String)> = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let old_name = k.extract::<String>()?;
            let new_name = v.extract::<String>()?;
            renames.push((old_name, new_name));
        }
        self.inner
            .with_columns_renamed(&renames)
            .map(|df| self.wrap_renamed_columns_derived(df, &renames))
            .map_err(to_py_err)
    }

    #[pyo3(name = "withColumnsRenamed")]
    fn with_columns_renamed_camel(&self, cols_map: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        self.with_columns_renamed(cols_map)
    }

    #[pyo3(name = "withColumns")]
    fn with_columns(&self, cols_map: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let dict = cols_map.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "withColumns expects a dict of {name: Column}",
            )
        })?;
        let mut exprs: Vec<(String, Column)> = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let name = k.extract::<String>()?;
            let col = py_any_to_column(&v)?;
            exprs.push((name, col));
        }
        self.inner
            .with_columns(&exprs)
            .map(|df| self.wrap_with_columns_derived(df, &exprs))
            .map_err(to_py_err)
    }

    #[pyo3(name = "writeTo")]
    fn write_to(&self, _table: &str) -> PyResult<()> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "writeTo (DataFrameWriterV2) is not yet implemented",
        ))
    }

    #[getter]
    fn write(slf: PyRef<Self>) -> PyDataFrameWriter {
        let py = slf.py();
        let active_actions = slf.active_actions.clone();
        let df = slf.into_py_any(py).unwrap();
        PyDataFrameWriter::new(df, active_actions)
    }

    fn create_or_replace_temp_view(&self, session: &PySparkSession, name: &str) {
        session
            .inner
            .create_or_replace_temp_view(name, self.inner.clone());
    }

    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<PyObject> {
        let names = self.inner.columns().map_err(to_py_err)?;
        let py_list = PyList::new(py, names.clone())?;
        let types_mod = PyModule::import(py, "sparkless.sql.types")?;
        let wrap = types_mod.getattr("_ColumnsList")?.call1((py_list,))?;
        wrap.into_py_any(py)
    }

    #[pyo3(name = "toDF", signature = (*col_names))]
    fn to_df(&self, col_names: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        if col_names.is_empty() {
            return Ok(self.wrap_derived(self.inner.clone()));
        }
        let mut df = self.inner.clone();
        let current = df.columns().map_err(to_py_err)?;
        for (i, item) in col_names.iter().enumerate() {
            if i >= current.len() {
                break;
            }
            let new_name: String = item.extract()?;
            df = df
                .with_column_renamed(&current[i], &new_name)
                .map_err(to_py_err)?;
        }
        let renames = current
            .iter()
            .zip(col_names.iter())
            .map(|(old_name, item)| Ok((old_name.clone(), item.extract::<String>()?)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(self.wrap_renamed_columns_derived(df, &renames))
    }

    #[pyo3(name = "toPandas")]
    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.check_mixed_row_shape()?;
        let action = self.begin_action();
        let token = action.token.clone();
        let materialization_token = token.clone();
        let inner = self.inner.clone();
        let array_struct_paths = self.arrow_array_struct_paths.clone();
        let pd = PyModule::import(py, "pandas")?;
        let (_, rows, schema) = py
            .detach(move || {
                inner.collect_as_json_rows_with_names_and_array_struct_columns_with_cancellation(
                    &array_struct_paths,
                    &materialization_token,
                )
            })
            .map_err(to_py_err)?;
        let cols = schema
            .logical_fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        let dtype_by_name: std::collections::HashMap<String, DataType> = schema
            .logical_fields()
            .iter()
            .map(|field| (field.name.clone(), field.data_type.clone()))
            .collect();
        let datetime_cls = PyModule::import(py, "datetime")?.getattr("datetime")?;
        let date_cls = PyModule::import(py, "datetime")?.getattr("date")?;
        let data_dict = PyDict::new(py);
        for col_name in &cols {
            token.check_cancelled().map_err(to_py_err)?;
            let vals = PyList::empty(py);
            for (row_index, row) in rows.iter().enumerate() {
                token.check_cancelled().map_err(to_py_err)?;
                let v = row.get(col_name).unwrap_or(&JsonValue::Null);
                vals.append(json_value_to_py_with_schema(
                    py,
                    v,
                    dtype_by_name.get(col_name),
                    &datetime_cls,
                    &date_cls,
                    Some(col_name),
                    Some(&cols),
                )?)?;
                if row_index % 32 == 31 {
                    yield_to_python_control_threads(py);
                    token.check_cancelled().map_err(to_py_err)?;
                }
            }
            yield_to_python_control_threads(py);
            token.check_cancelled().map_err(to_py_err)?;
            data_dict.set_item(col_name, vals)?;
        }
        let df = pd.call_method1("DataFrame", (data_dict,))?;
        token.check_cancelled().map_err(to_py_err)?;
        df.into_py_any(py)
    }

    /// Export collected values to a PyArrow Table, retaining decimal precision and scale.
    #[pyo3(name = "toArrow")]
    fn to_arrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.check_mixed_row_shape()?;
        let action = self.begin_action();
        let token = action.token.clone();
        let rows = self.collect(py)?;
        token.check_cancelled().map_err(to_py_err)?;
        let schema = self.inner.schema_engine().map_err(to_py_err)?;
        let pa = PyModule::import(py, "pyarrow")?;
        let mut source_arrow_fields = std::collections::HashMap::new();
        if let Some(source_schema) = &self.arrow_schema_cache {
            for source_field in source_schema.bind(py).try_iter()? {
                let source_field = source_field?;
                let name = source_field.getattr("name")?.extract::<String>()?;
                source_arrow_fields.insert(name, source_field.unbind());
            }
        }
        let arrays = PyList::empty(py);
        let arrow_fields = PyList::empty(py);
        let mut output_names = Vec::with_capacity(schema.logical_fields().len());
        for field in schema.logical_fields() {
            token.check_cancelled().map_err(to_py_err)?;
            output_names.push(field.name.clone());
            let values = PyList::empty(py);
            let field_path = arrow_field_path("", &field.name);
            let map_shaped_dtype = matches!(
                &field.data_type,
                DataType::Array(element_type)
                    if matches!(element_type.as_ref(), DataType::LogicalStruct(fields)
                        if fields.iter().any(|field| field.name == "key")
                            && fields.iter().any(|field| field.name == "value"))
            );
            let map_value_present = if map_shaped_dtype {
                let mut found = false;
                for (row_index, row) in rows.iter().enumerate() {
                    token.check_cancelled().map_err(to_py_err)?;
                    let dict = row.bind(py).call_method0("asDict")?;
                    let value = dict.get_item(field.name.as_str())?;
                    if !value.is_none() {
                        found = value.is_instance_of::<PyDict>();
                        break;
                    }
                    if row_index % 32 == 31 {
                        yield_to_python_control_threads(py);
                        token.check_cancelled().map_err(to_py_err)?;
                    }
                }
                found
            } else {
                false
            };
            // The cached source schema can be stale after withColumn replaces a
            // map with an array that has the same physical key/value shape.
            // Derived logical paths are authoritative; infer only for uncached,
            // dictionary-shaped values.
            let is_map = self.logical_map_paths.contains(&field_path)
                || self.logical_map_columns.contains(&field.name)
                || (map_shaped_dtype && map_value_present);
            let mut field_map_paths = self.logical_map_paths.clone();
            if is_map {
                field_map_paths.insert(field_path.clone());
            }
            for (row_index, row) in rows.iter().enumerate() {
                token.check_cancelled().map_err(to_py_err)?;
                let row = row.bind(py);
                let dict = row.call_method0("asDict")?;
                let value = dict.get_item(field.name.as_str())?;
                let value = normalize_arrow_value_with_maps(
                    py,
                    &value,
                    &field.data_type,
                    &field_path,
                    &field_map_paths,
                )?;
                values.append(value.bind(py))?;
                if row_index % 32 == 31 {
                    yield_to_python_control_threads(py);
                    token.check_cancelled().map_err(to_py_err)?;
                }
            }
            yield_to_python_control_threads(py);
            token.check_cancelled().map_err(to_py_err)?;
            let current_arrow_type = spark_type_to_arrow_type_with_maps(
                py,
                &pa,
                &field.data_type,
                &field_path,
                &field_map_paths,
            )?;
            let source_field = source_arrow_fields
                .get(&field.name)
                .filter(|_| !self.arrow_schema_invalidated_fields.contains(&field.name));
            // Constructing with the original Arrow type retains details that Spark's
            // logical schema cannot express (timezone, nested field nullability and
            // field metadata). If the source type no longer accepts these values,
            // fall back to the current logical type.
            let compatible_source = match source_field {
                Some(source_field) => arrow_types_compatible(
                    py,
                    &pa,
                    &source_field.bind(py).getattr("type")?,
                    current_arrow_type.bind(py),
                )?,
                None => false,
            };
            let source_array = if compatible_source {
                let source_field = source_field.expect("compatible source field exists");
                pa.getattr("array")?
                    .call1((values.clone(), source_field.bind(py).getattr("type")?))
                    .ok()
            } else {
                None
            };
            let (array, arrow_type, reused_source_type) = match source_array {
                Some(array) => {
                    let arrow_type = source_field
                        .expect("source array has source field")
                        .bind(py)
                        .getattr("type")?
                        .into_py_any(py)?;
                    (array, arrow_type, true)
                }
                None => {
                    let arrow_type = current_arrow_type.clone_ref(py);
                    let array = pa.getattr("array")?.call1((values, arrow_type.bind(py)))?;
                    (array, arrow_type, false)
                }
            };
            arrays.append(&array)?;
            let source_field_nullable = source_field
                .map(|source_field| source_field.bind(py).getattr("nullable"))
                .transpose()?
                .map(|nullable| nullable.extract::<bool>())
                .transpose()?
                .unwrap_or(true);
            let has_nulls = array.getattr("null_count")?.extract::<usize>()? > 0;
            if reused_source_type && (source_field_nullable || !has_nulls) {
                arrow_fields.append(source_field.expect("reused source field exists").bind(py))?;
            } else if reused_source_type && has_nulls {
                let field_kwargs = PyDict::new(py);
                field_kwargs.set_item("nullable", true)?;
                if let Some(source_field) = source_field {
                    let metadata = source_field.bind(py).getattr("metadata")?;
                    if !metadata.is_none() {
                        field_kwargs.set_item("metadata", metadata)?;
                    }
                }
                arrow_fields.append(
                    pa.getattr("field")?
                        .call((field.name.as_str(), arrow_type), Some(&field_kwargs))?,
                )?;
            } else {
                arrow_fields.append(
                    pa.getattr("field")?
                        .call1((field.name.as_str(), current_arrow_type))?,
                )?;
            }
        }
        token.check_cancelled().map_err(to_py_err)?;
        let schema_kwargs = PyDict::new(py);
        if let Some(source_schema) = &self.arrow_schema_cache {
            let source_schema = source_schema.bind(py);
            let source_names = source_schema.getattr("names")?.extract::<Vec<String>>()?;
            if source_names == output_names {
                let metadata = source_schema.getattr("metadata")?;
                if !metadata.is_none() {
                    schema_kwargs.set_item("metadata", metadata)?;
                }
            }
        }
        let arrow_schema = pa
            .getattr("schema")?
            .call((arrow_fields,), Some(&schema_kwargs))?;
        let table_kwargs = PyDict::new(py);
        table_kwargs.set_item("schema", arrow_schema)?;
        let table = pa
            .getattr("Table")?
            .getattr("from_arrays")?
            .call((arrays,), Some(&table_kwargs))?
            .into_py_any(py)?;
        token.check_cancelled().map_err(to_py_err)?;
        Ok(table)
    }

    /// Yield PyArrow RecordBatches without materializing the full DataFrame.
    ///
    /// Each returned batch stays within both limits. If one row alone exceeds
    /// max_bytes, iteration raises ValueError. Call close(), or use the iterator
    /// as a context manager, to stop the query when the consumer is done early.
    #[pyo3(name = "toArrowBatches", signature = (max_rows=10000, max_bytes=67108864))]
    fn to_arrow_batches(
        &self,
        py: Python<'_>,
        max_rows: usize,
        max_bytes: usize,
    ) -> PyResult<PyObject> {
        self.create_arrow_batches(py, max_rows, max_bytes)
    }

    #[pyo3(name = "to_arrow_batches", signature = (max_rows=10000, max_bytes=67108864))]
    fn to_arrow_batches_snake(
        &self,
        py: Python<'_>,
        max_rows: usize,
        max_bytes: usize,
    ) -> PyResult<PyObject> {
        self.create_arrow_batches(py, max_rows, max_bytes)
    }

    #[getter]
    fn dtypes(&self) -> PyResult<Vec<(String, String)>> {
        let schema = self.inner.schema_engine().map_err(to_py_err)?;
        Ok(schema
            .logical_fields()
            .iter()
            .map(|f| {
                let ty = match &f.data_type {
                    DataType::String => "string".to_string(),
                    DataType::Short => "smallint".to_string(),
                    DataType::Integer => "int".to_string(),
                    DataType::Long => "long".to_string(),
                    DataType::Float => "float".to_string(),
                    DataType::Double => "double".to_string(),
                    DataType::Decimal(precision, scale) => {
                        format!("decimal({precision},{scale})")
                    }
                    DataType::Boolean => "boolean".to_string(),
                    DataType::Date => "date".to_string(),
                    DataType::Timestamp => "timestamp".to_string(),
                    DataType::TimestampNtz => "timestamp_ntz".to_string(),
                    DataType::Binary => "binary".to_string(),
                    DataType::Array(_) => "array".to_string(),
                    DataType::Map(_, _) => "map".to_string(),
                    DataType::LogicalStruct(_) => "struct".to_string(),
                    _ => "unknown".to_string(),
                };
                (f.name.clone(), ty)
            })
            .collect())
    }

    fn coalesce(&self, _n: usize) -> PyResult<PyDataFrame> {
        Ok(self.wrap_derived(self.inner.clone()))
    }

    fn repartition(&self, _n: usize) -> PyResult<PyDataFrame> {
        Ok(self.wrap_derived(self.inner.clone()))
    }

    #[pyo3(name = "isEmpty")]
    fn is_empty(&self, py: Python<'_>) -> PyResult<bool> {
        self.check_mixed_row_shape()?;
        let action = self.begin_action();
        let token = action.token.clone();
        let limited = self.inner.limit(1).map_err(to_py_err)?;
        py.detach(move || limited.count_with_cancellation(&token))
            .map(|count| count == 0)
            .map_err(to_py_err)
    }

    fn take(&self, py: Python<'_>, n: usize) -> PyResult<Vec<PyObject>> {
        let limited = self.inner.limit(n).map_err(to_py_err)?;
        let mut df = self.wrap_derived(limited);
        df.active_actions = self.active_actions.clone();
        df.collect(py)
    }

    fn __repr__(&self) -> PyResult<String> {
        let cols = self.inner.columns().map_err(to_py_err)?;
        Ok(format!("DataFrame[{}]", cols.join(", ")))
    }
}
