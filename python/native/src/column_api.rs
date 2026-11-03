//! Python Column methods and expression adaptation.

use super::*;

#[pymethods]
impl PyColumn {
    fn alias(&self, name: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.alias(name),
        }
    }

    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns (udf_name, arg_column_names) if this column is a Python UDF call, else None. Used by Python UDF executor.
    fn get_udf_call_info(&self) -> Option<(String, Vec<String>)> {
        self.inner.udf_call_info()
    }

    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.add_pyspark(&rhs),
        })
    }

    fn __radd__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.add_pyspark(&self.inner),
        })
    }

    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.subtract_pyspark(&rhs),
        })
    }

    fn __rsub__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.subtract_pyspark(&self.inner),
        })
    }

    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.multiply_pyspark(&rhs),
        })
    }

    fn __rmul__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.multiply_pyspark(&self.inner),
        })
    }

    /// col ** 2 (PySpark pow). Supports int, float, or Column exponent. Ignores modulo (PySpark has no modulo for pow).
    fn __pow__(
        &self,
        other: &Bound<'_, PyAny>,
        _modulo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyColumn> {
        if let Ok(py_col) = other.cast::<PyColumn>() {
            return Ok(PyColumn {
                inner: self.inner.pow_with(&py_col.borrow().inner),
            });
        }
        if let Ok(exp_i) = other.extract::<i64>() {
            return Ok(PyColumn {
                inner: self.inner.pow(exp_i),
            });
        }
        if let Ok(exp_i) = other.extract::<i32>() {
            return Ok(PyColumn {
                inner: self.inner.pow(exp_i as i64),
            });
        }
        if let Ok(_exp_f) = other.extract::<f64>() {
            let exp_col = py_any_to_column(other)?;
            return Ok(PyColumn {
                inner: self.inner.pow_with(&exp_col),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "pow exponent must be int, float, or Column",
        ))
    }

    fn __rpow__(
        &self,
        other: &Bound<'_, PyAny>,
        _modulo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyColumn> {
        // Support Python scalar ** PyColumn, e.g. 2.0 ** (-col("Value")) (issue #291/#1082).
        // Treat the Python value as the base column and this PyColumn as the exponent.
        let base_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: base_col.pow_with(&self.inner),
        })
    }

    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.divide_pyspark(&rhs),
        })
    }

    fn __rtruediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.divide_pyspark(&self.inner),
        })
    }

    fn __mod__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.mod_pyspark(&rhs),
        })
    }

    fn __rmod__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.mod_pyspark(&self.inner),
        })
    }

    fn __neg__(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.negate(),
        }
    }

    fn __and__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.and_(&rhs),
        })
    }

    fn __rand__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.and_(&self.inner),
        })
    }

    fn __or__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        // PySpark: col | col in filters is logical OR (e.g. (col("a") == 1) | (col("b") == 2)).
        // Use or_() not bit_or() so filter coercion walks comparison sub-expressions (#1545).
        Ok(PyColumn {
            inner: self.inner.or_(&rhs),
        })
    }

    fn __ror__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.or_(&self.inner),
        })
    }

    fn __invert__(&self) -> PyColumn {
        PyColumn {
            // For boolean expressions (e.g. ~(df.a == 20)), behave like logical NOT.
            // Numeric bitwise NOT is exposed via F.bitwise_not(...) instead.
            inner: self.inner.logical_not(),
        }
    }

    fn __getitem__(&self, key: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        if let Ok(idx) = key.extract::<i64>() {
            return Ok(PyColumn {
                inner: self.inner.get_item(idx),
            });
        }
        if let Ok(name) = key.extract::<String>() {
            return Ok(PyColumn {
                inner: self.inner.get_field(&name),
            });
        }
        if let Ok(py_col) = key.cast::<PyColumn>() {
            return Ok(PyColumn {
                inner: self.inner.get(&py_col.borrow().inner),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Column subscript key must be int, str, or Column",
        ))
    }

    /// Logical AND of two boolean columns. PySpark and_.
    fn and_(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.and_(&other.inner),
        }
    }

    /// Logical OR of two boolean columns. PySpark or_.
    fn or_(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.or_(&other.inner),
        }
    }

    fn gt(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.gt(other.inner.clone().into_expr()),
        }
    }

    fn ge(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.gt_eq(other.inner.clone().into_expr()),
        }
    }

    fn lt(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.lt(other.inner.clone().into_expr()),
        }
    }

    fn le(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.lt_eq(other.inner.clone().into_expr()),
        }
    }

    fn eq(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.eq(other.inner.clone().into_expr()),
        }
    }

    /// Null-safe equality (NULL <=> NULL returns True). PySpark eqNullSafe.
    /// Accepts Column or scalar (int, float, str, bool, None) so .eqNullSafe(lit(x)) or .eqNullSafe(x) work.
    fn eq_null_safe(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        // Special-case Python string scalars so that int-column vs string-literal
        // (e.g. col("val").eqNullSafe("123")) behaves like PySpark: attempt to parse
        // the string as a number and compare numerically when possible (#1471).
        if let Ok(s) = other.extract::<String>() {
            if let Ok(i) = s.parse::<i64>() {
                let other_col = robin_sparkless::functions::lit_i64(i);
                return Ok(PyColumn {
                    inner: self.inner.eq_null_safe(&other_col),
                });
            }
            if let Ok(f) = s.parse::<f64>() {
                let other_col = robin_sparkless::functions::lit_f64(f);
                return Ok(PyColumn {
                    inner: self.inner.eq_null_safe(&other_col),
                });
            }
            // Fall through: treat as string literal for non-numeric text ("test", etc.).
        }

        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.eq_null_safe(&other_col),
        })
    }

    /// PySpark camelCase alias for eq_null_safe.
    #[pyo3(name = "eqNullSafe")]
    fn eq_null_safe_camel(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        self.eq_null_safe(other)
    }

    /// True if column value is between lower and upper (inclusive). PySpark between(low, high).
    /// Accepts Column or scalar (int, float, str, bool, None) for lower/upper.
    fn between(&self, lower: &Bound<'_, PyAny>, upper: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lower_col = py_any_to_column(lower)?;
        let upper_col = py_any_to_column(upper)?;
        Ok(PyColumn {
            inner: self.inner.between(&lower_col, &upper_col),
        })
    }

    /// Rich comparison for Column (PySpark returns Column from comparisons, not bool).
    /// UDF columns compared to literals route through PyUdfFilterColumn for filter() expansion.
    fn __richcmp__(
        slf: PyRef<Self>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let op_str = match op {
            CompareOp::Eq => "eq",
            CompareOp::Ne => "ne",
            CompareOp::Lt => "lt",
            CompareOp::Le => "le",
            CompareOp::Gt => "gt",
            CompareOp::Ge => "ge",
        };
        if slf.inner.udf_call_info().is_some() {
            if let Some(obj) = py_udf_filter_column_from_comparison(py, &slf.inner, other, op_str)?
            {
                return Ok(obj);
            }
        }
        let other_col = match py_any_to_column(other) {
            Ok(c) => c,
            Err(_) => return Ok(py.NotImplemented()),
        };
        let inner = &slf.inner;
        let result = match op {
            CompareOp::Eq => inner.eq(other_col.into_expr()),
            CompareOp::Ne => inner.neq(other_col.into_expr()),
            CompareOp::Lt => inner.lt(other_col.into_expr()),
            CompareOp::Le => inner.lt_eq(other_col.into_expr()),
            CompareOp::Gt => inner.gt(other_col.into_expr()),
            CompareOp::Ge => inner.gt_eq(other_col.into_expr()),
        };
        PyColumn { inner: result }.into_py_any(py)
    }

    fn is_null(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.is_null(),
        }
    }

    #[pyo3(name = "isNull")]
    fn is_null_camel(&self) -> PyColumn {
        self.is_null()
    }

    /// PySpark-style snake_case alias for isNull() (used in some upstream tests).
    #[pyo3(name = "isnull")]
    fn is_null_snake(&self) -> PyColumn {
        self.is_null()
    }

    #[pyo3(name = "getItem")]
    fn get_item_camel(&self, index_or_key: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        if let Ok(i) = index_or_key.extract::<i64>() {
            return Ok(PyColumn {
                inner: self.inner.get_item(i),
            });
        }
        if let Ok(s) = index_or_key.extract::<String>() {
            let key_col = robin_sparkless::functions::lit_str(&s);
            return Ok(PyColumn {
                inner: self.inner.get(&key_col),
            });
        }
        if let Ok(key_col) = index_or_key.cast::<PyColumn>() {
            return Ok(PyColumn {
                inner: self.inner.get(&key_col.borrow().inner),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "getItem expects int (array index), str (map key), or Column (map key)",
        ))
    }

    /// Get struct field by name or array element by index (PySpark Column.getField).
    /// Accepts int (array index, 0-based) or str (struct field name). Issue #1066.
    fn get_field(&self, name_or_index: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        if let Ok(i) = name_or_index.extract::<i64>() {
            return Ok(PyColumn {
                inner: self.inner.get_item(i),
            });
        }
        if let Ok(name) = name_or_index.extract::<String>() {
            return Ok(PyColumn {
                inner: self.inner.get_field(&name),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "getField expects int (array index) or str (struct field name)",
        ))
    }

    #[pyo3(name = "getField")]
    fn get_field_camel(&self, name_or_index: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        self.get_field(name_or_index)
    }

    /// String column contains substring (PySpark Column.contains).
    fn contains(&self, literal: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.contains(literal),
        }
    }

    /// col[key] -> map get or array getItem. Key: int (array index), str (map key), or Column (map key).
    fn array_distinct(&self) -> PyColumn {
        PyColumn {
            inner: functions::array_distinct(&self.inner),
        }
    }

    /// Remove null elements from list (PySpark array_compact).
    fn array_compact(&self) -> PyColumn {
        PyColumn {
            inner: functions::array_compact(&self.inner),
        }
    }

    fn posexplode(&self) -> (PyColumn, PyColumn) {
        let (pos, val) = functions::posexplode(&self.inner);
        (PyColumn { inner: pos }, PyColumn { inner: val })
    }

    #[pyo3(signature = (*values))]
    fn isin(&self, values: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
        let mut expanded: Vec<Py<PyAny>> = Vec::new();
        let mut has_none = false;
        let py = values.py();

        for item in values.iter() {
            if let Ok(list) = item.cast::<PyList>() {
                for x in list.iter() {
                    if x.is_none() {
                        has_none = true;
                    } else {
                        expanded.push(x.unbind());
                    }
                }
            } else if item.is_none() {
                has_none = true;
            } else {
                expanded.push(item.unbind());
            }
        }

        // If no non-null values in list:
        // - If None was present: all results are NULL (uncertain comparison)
        // - If None was not present: all results are false (no match possible)
        if expanded.is_empty() {
            return Ok(PyColumn {
                inner: if has_none {
                    robin_sparkless::functions::lit_null_untyped()
                } else {
                    robin_sparkless::functions::lit_bool(false)
                },
            });
        }

        // Build the basic isin result with non-null values
        let first = expanded[0].bind(py);
        let base_result = if first.extract::<i64>().is_ok() {
            let vals: Vec<i64> = expanded
                .iter()
                .map(|v| v.bind(py).extract::<i64>())
                .collect::<PyResult<_>>()
                .map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin values must have a consistent type",
                    )
                })?;
            functions::isin_i64(&self.inner, &vals)
        } else {
            let first_type = first.get_type();
            if expanded
                .iter()
                .any(|v| !v.bind(py).is_exact_instance(first_type.as_any()))
            {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "isin values must have a consistent type",
                ));
            }
            let vals: Vec<String> = expanded.iter().map(|v| v.bind(py).to_string()).collect();
            let refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
            functions::isin_str(&self.inner, &refs)
        };

        // If None was in the list, non-matching rows should return NULL (uncertain)
        // PySpark behavior: match -> true, no match but None in list -> NULL
        let result = if has_none {
            // when(base_result).then(true).otherwise(NULL)
            functions::when(&base_result)
                .then(&robin_sparkless::functions::lit_bool(true))
                .otherwise(&robin_sparkless::functions::lit_null_untyped())
        } else {
            base_result
        };

        Ok(PyColumn { inner: result })
    }

    fn is_not_null(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.is_not_null(),
        }
    }

    #[pyo3(name = "isNotNull")]
    fn is_not_null_camel(&self) -> PyColumn {
        self.is_not_null()
    }

    /// PySpark-style snake_case alias for isNotNull() (used in some upstream tests).
    #[pyo3(name = "isnotnull")]
    fn is_not_null_snake(&self) -> PyColumn {
        self.is_not_null()
    }

    fn asc(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc(),
        }
    }

    fn desc(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc(),
        }
    }

    /// Descending sort, nulls last. PySpark desc_nulls_last.
    fn desc_nulls_last(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc_nulls_last(),
        }
    }

    /// PySpark camelCase alias for desc_nulls_last.
    #[pyo3(name = "descNullsLast")]
    fn desc_nulls_last_camel(&self) -> PySortOrder {
        self.desc_nulls_last()
    }

    fn upper(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.upper(),
        }
    }

    fn rlike(&self, pattern: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.regexp_like(pattern),
        }
    }

    /// SQL LIKE pattern match (PySpark like). Supports % and _ wildcards.
    fn like(&self, pattern: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.like(pattern, None),
        }
    }

    /// True if string starts with prefix (PySpark startswith).
    fn startswith(&self, prefix: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.startswith(prefix),
        }
    }

    /// True if string ends with suffix (PySpark endswith).
    fn endswith(&self, suffix: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.endswith(suffix),
        }
    }

    /// Extract first match of regex pattern. PySpark regexp_extract. idx=0 is full match.
    #[pyo3(signature = (pattern, idx=0))]
    fn regexp_extract(&self, pattern: &str, idx: usize) -> PyColumn {
        PyColumn {
            inner: self.inner.regexp_extract(pattern, idx),
        }
    }

    /// Replace all matches of regex pattern. PySpark regexp_replace (global replace).
    fn regexp_replace(&self, pattern: &str, replacement: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.regexp_replace(pattern, replacement),
        }
    }

    /// PySpark Column.replace(search, replacement) - literal string replace.
    /// PySpark Column.replace: (search, replacement) or replace({old: new, ...}) or replace([(old, new), ...]).
    #[pyo3(signature = (to_replace, replacement=None))]
    fn replace(
        &self,
        to_replace: &Bound<'_, PyAny>,
        replacement: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyColumn> {
        if let Ok(search) = to_replace.extract::<String>() {
            let rep = replacement.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "replace() missing 1 required positional argument: 'replacement'. When to_replace is not a dict, value must be provided.",
                )
            })?;
            let rep_str: String = rep.extract().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("replacement must be str")
            })?;
            return Ok(PyColumn {
                inner: self.inner.replace(&search, &rep_str),
            });
        }
        if let Ok(dict) = to_replace.cast::<PyDict>() {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for (k, v) in dict.iter() {
                let s: String = k.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("replace dict keys must be str")
                })?;
                let r: String = v.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "replace dict values must be str",
                    )
                })?;
                pairs.push((s, r));
            }
            return Ok(PyColumn {
                inner: self.inner.replace_many(&pairs),
            });
        }
        if let Ok(list) = to_replace.cast::<PyList>() {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for item in list.iter() {
                let tup = item.cast::<PyTuple>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "replace list must contain (old, new) tuples",
                    )
                })?;
                if tup.len() != 2 {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "replace tuple must be (old, new)",
                    ));
                }
                let s: String = tup.get_item(0)?.extract()?;
                let r: String = tup.get_item(1)?.extract()?;
                pairs.push((s, r));
            }
            return Ok(PyColumn {
                inner: self.inner.replace_many(&pairs),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "replace to_replace must be str, dict, or list of (str, str) tuples",
        ))
    }

    fn lower(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.lower(),
        }
    }

    #[pyo3(signature = (start, length=None))]
    fn substr(
        &self,
        start: &Bound<'_, PyAny>,
        length: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyColumn> {
        let start_col = py_any_to_i64_or_column(start)?;
        let length_col = length.map(py_any_to_i64_or_column).transpose()?;
        Ok(PyColumn {
            inner: self.inner.substr_with(&start_col, length_col.as_ref()),
        })
    }

    /// PySpark parity #1249: Column has .length attribute but it is not callable; use F.length(col) instead.
    /// Returning self so that col.length() raises TypeError (column not callable).
    #[getter]
    fn length(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Split string by delimiter. PySpark split. limit=-1 means no limit.
    #[pyo3(signature = (delimiter, limit=-1))]
    fn split(&self, delimiter: &str, limit: i32) -> PyColumn {
        let lim = if limit < 0 { None } else { Some(limit) };
        PyColumn {
            inner: self.inner.split(delimiter, lim),
        }
    }

    fn floor(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.floor(),
        }
    }

    #[pyo3(signature = (scale=0))]
    fn round(&self, scale: i32) -> PyColumn {
        PyColumn {
            inner: self.inner.round(scale),
        }
    }

    fn ltrim(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.ltrim(),
        }
    }

    fn rtrim(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.rtrim(),
        }
    }

    fn second(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.second(),
        }
    }

    fn hour(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.hour(),
        }
    }

    fn minute(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.minute(),
        }
    }

    fn soundex(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.soundex(),
        }
    }

    fn repeat(&self, n: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let n = py_any_to_i64_or_column(n)?;
        Ok(PyColumn {
            inner: self.inner.repeat_dynamic(&n),
        })
    }

    fn reverse(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.reverse(),
        }
    }

    fn exp(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.exp(),
        }
    }

    fn crc32(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.crc32(),
        }
    }

    fn xxhash64(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.xxhash64(),
        }
    }

    fn initcap(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.initcap(),
        }
    }

    fn levenshtein(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.levenshtein(&other.inner),
        }
    }

    /// PySpark parity (#1233): Column.try_cast is not supported; raises TypeError like PySpark.
    fn try_cast(&self, _type_name: &str) -> PyResult<PyColumn> {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "Column object is not callable",
        ))
    }

    /// Add or replace a struct field. PySpark withField.
    fn with_field(&self, name: &str, value: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.with_field(name, &value.inner),
        }
    }

    /// PySpark camelCase alias for with_field.
    #[pyo3(name = "withField")]
    fn with_field_camel(&self, name: &str, value: &PyColumn) -> PyColumn {
        self.with_field(name, value)
    }

    fn asinh(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.asinh(),
        }
    }

    fn atanh(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.atanh(),
        }
    }

    fn cosh(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.cosh(),
        }
    }

    fn sinh(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.sinh(),
        }
    }

    fn last_day(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.last_day(),
        }
    }

    #[pyo3(signature = (start, round_off=true))]
    fn months_between(&self, start: &PyColumn, round_off: bool) -> PyColumn {
        PyColumn {
            inner: self.inner.months_between(&start.inner, round_off),
        }
    }

    fn timestamp_seconds(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.timestamp_seconds(),
        }
    }

    fn to_utc_timestamp(&self, tz: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.to_utc_timestamp(tz),
        }
    }

    fn trim(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.trim(),
        }
    }

    fn cast(&self, type_name: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let name = resolve_cast_type_name(type_name)?;
        self.inner
            .cast_to(&name)
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
    }

    /// PySpark astype(dtype): alias for cast(dtype).
    fn astype(&self, dtype: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        self.cast(dtype)
    }

    /// Ascending sort, nulls first. PySpark asc_nulls_first.
    fn asc_nulls_first(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc_nulls_first(),
        }
    }

    /// Ascending sort, nulls last. PySpark asc_nulls_last.
    fn asc_nulls_last(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc_nulls_last(),
        }
    }

    /// Descending sort, nulls first. PySpark desc_nulls_first.
    fn desc_nulls_first(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc_nulls_first(),
        }
    }

    fn row_number(&self, descending: bool) -> PyColumn {
        PyColumn {
            inner: self.inner.row_number(descending),
        }
    }

    fn rank(&self, descending: bool) -> PyColumn {
        PyColumn {
            inner: self.inner.rank(descending),
        }
    }

    fn dense_rank(&self, descending: bool) -> PyColumn {
        PyColumn {
            inner: self.inner.dense_rank(descending),
        }
    }

    #[pyo3(signature = (offset=1))]
    fn lag(&self, offset: i64) -> PyColumn {
        PyColumn {
            inner: self.inner.lag(offset),
        }
    }

    #[pyo3(signature = (offset=1))]
    fn lead(&self, offset: i64) -> PyColumn {
        PyColumn {
            inner: self.inner.lead(offset),
        }
    }

    fn first_value(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.first_value(),
        }
    }

    fn last_value(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.last_value(),
        }
    }

    fn over(&self, py: Python<'_>, window: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let funcs = PyModule::import(py, "sparkless.sql.functions")?;
        let extract = funcs.getattr("_window_spec_to_partition_order")?;
        let result = extract.call1((window, false))?;
        let tuple = result.cast::<PyTuple>()?;
        let len = tuple.len();
        if len < 4 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "_window_spec_to_partition_order expected at least 4 elements, got {}",
                len
            )));
        }
        let partition_by: Vec<String> = tuple.get_item(0)?.extract()?;
        let order_by: Vec<String> = tuple.get_item(1)?.extract()?;
        let use_running_aggregate: bool = tuple.get_item(2)?.extract()?;
        let is_full_partition_frame: bool = tuple.get_item(3)?.extract()?;
        // 5th element (frame) only present when len >= 5 (current contract); older returns had 4 elements.
        let frame: Option<(String, i64, i64)> = if len >= 5 {
            let frame_any = tuple.get_item(4)?;
            if frame_any.is_none() {
                None
            } else {
                let ft = frame_any.cast::<PyTuple>()?;
                let kind: String = ft.get_item(0)?.extract()?;
                let start: i64 = ft.get_item(1)?.extract()?;
                let end: i64 = ft.get_item(2)?.extract()?;
                Some((kind, start, end))
            }
        } else {
            None
        };
        let partition_strs: Vec<&str> = partition_by.iter().map(String::as_str).collect();
        let frame_ref: Option<(&str, i64, i64)> = frame
            .as_ref()
            .map(|(k, s, e): &(String, i64, i64)| (k.as_str(), *s, *e));
        self.inner
            .over_window(
                &partition_strs[..],
                &order_by,
                use_running_aggregate,
                is_full_partition_frame,
                frame_ref,
            )
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
    }
}
