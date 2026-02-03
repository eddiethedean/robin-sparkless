//! Python bindings for robin-sparkless (PyO3).
//! Compiled only when the `pyo3` feature is enabled.

use crate::column::Column as RsColumn;
use crate::dataframe::JoinType;
use crate::functions::{
    acos, acosh, add_months, array_compact, array_distinct, ascii, asin, asinh, atan, atan2, atanh,
    base64, cast as rs_cast, cbrt, ceiling, chr, contains, cos, cosh, day, dayofmonth, dayofweek,
    dayofyear, degrees, endswith, expm1, find_in_set, format_number, format_string,
    greatest as rs_greatest, hypot, ifnull, ilike, isnan as rs_isnan, isnotnull, isnull, lcase,
    least as rs_least, left, like, ln, log10, log1p, log2, md5, months_between, next_day, nvl,
    nvl2, overlay, position as rs_position, power, quarter, radians, regexp_count, regexp_instr,
    regexp_substr, replace as rs_replace, right, rint, rlike, sha1, sha2, signum, sin, sinh,
    split_part, startswith, substr, tan, tanh, to_degrees, to_radians, try_cast as rs_try_cast,
    ucase, unbase64, weekofyear,
};
use crate::functions::{avg, coalesce, col as rs_col, count, max, min, sum as rs_sum};
use crate::{DataFrame, GroupedData, SparkSession};
use polars::prelude::Expr;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::Path;

/// Python module entry point.
#[pymodule]
fn robin_sparkless(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySparkSession>()?;
    m.add_class::<PySparkSessionBuilder>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyDataFrameStat>()?;
    m.add_class::<PyDataFrameNa>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyWhenBuilder>()?;
    m.add_class::<PyThenBuilder>()?;
    m.add_class::<PyGroupedData>()?;
    m.add("col", wrap_pyfunction!(py_col, m)?)?;
    m.add("lit", wrap_pyfunction!(py_lit, m)?)?;
    m.add("when", wrap_pyfunction!(py_when, m)?)?;
    m.add("coalesce", wrap_pyfunction!(py_coalesce, m)?)?;
    m.add("sum", wrap_pyfunction!(py_sum, m)?)?;
    m.add("avg", wrap_pyfunction!(py_avg, m)?)?;
    m.add("min", wrap_pyfunction!(py_min, m)?)?;
    m.add("max", wrap_pyfunction!(py_max, m)?)?;
    m.add("count", wrap_pyfunction!(py_count, m)?)?;
    m.add("ascii", wrap_pyfunction!(py_ascii, m)?)?;
    m.add("format_number", wrap_pyfunction!(py_format_number, m)?)?;
    m.add("overlay", wrap_pyfunction!(py_overlay, m)?)?;
    m.add("position", wrap_pyfunction!(py_position, m)?)?;
    m.add("char", wrap_pyfunction!(py_char, m)?)?;
    m.add("chr", wrap_pyfunction!(py_chr, m)?)?;
    m.add("base64", wrap_pyfunction!(py_base64, m)?)?;
    m.add("unbase64", wrap_pyfunction!(py_unbase64, m)?)?;
    m.add("sha1", wrap_pyfunction!(py_sha1, m)?)?;
    m.add("sha2", wrap_pyfunction!(py_sha2, m)?)?;
    m.add("md5", wrap_pyfunction!(py_md5, m)?)?;
    m.add("array_compact", wrap_pyfunction!(py_array_compact, m)?)?;
    m.add("array_distinct", wrap_pyfunction!(py_array_distinct, m)?)?;
    // Phase 14: math, datetime, type/conditional
    m.add("sin", wrap_pyfunction!(py_sin, m)?)?;
    m.add("cos", wrap_pyfunction!(py_cos, m)?)?;
    m.add("tan", wrap_pyfunction!(py_tan, m)?)?;
    m.add("asin", wrap_pyfunction!(py_asin, m)?)?;
    m.add("acos", wrap_pyfunction!(py_acos, m)?)?;
    m.add("atan", wrap_pyfunction!(py_atan, m)?)?;
    m.add("atan2", wrap_pyfunction!(py_atan2, m)?)?;
    m.add("degrees", wrap_pyfunction!(py_degrees, m)?)?;
    m.add("radians", wrap_pyfunction!(py_radians, m)?)?;
    m.add("signum", wrap_pyfunction!(py_signum, m)?)?;
    m.add("quarter", wrap_pyfunction!(py_quarter, m)?)?;
    m.add("weekofyear", wrap_pyfunction!(py_weekofyear, m)?)?;
    m.add("dayofweek", wrap_pyfunction!(py_dayofweek, m)?)?;
    m.add("dayofyear", wrap_pyfunction!(py_dayofyear, m)?)?;
    m.add("add_months", wrap_pyfunction!(py_add_months, m)?)?;
    m.add("months_between", wrap_pyfunction!(py_months_between, m)?)?;
    m.add("next_day", wrap_pyfunction!(py_next_day, m)?)?;
    m.add("cast", wrap_pyfunction!(py_cast, m)?)?;
    m.add("try_cast", wrap_pyfunction!(py_try_cast, m)?)?;
    m.add("isnan", wrap_pyfunction!(py_isnan, m)?)?;
    m.add("greatest", wrap_pyfunction!(py_greatest, m)?)?;
    m.add("least", wrap_pyfunction!(py_least, m)?)?;
    // Phase 15 Batch 1: aliases and simple
    m.add("nvl", wrap_pyfunction!(py_nvl, m)?)?;
    m.add("ifnull", wrap_pyfunction!(py_ifnull, m)?)?;
    m.add("nvl2", wrap_pyfunction!(py_nvl2, m)?)?;
    m.add("substr", wrap_pyfunction!(py_substr, m)?)?;
    m.add("power", wrap_pyfunction!(py_power, m)?)?;
    m.add("ln", wrap_pyfunction!(py_ln, m)?)?;
    m.add("ceiling", wrap_pyfunction!(py_ceiling, m)?)?;
    m.add("lcase", wrap_pyfunction!(py_lcase, m)?)?;
    m.add("ucase", wrap_pyfunction!(py_ucase, m)?)?;
    m.add("day", wrap_pyfunction!(py_day, m)?)?;
    m.add("dayofmonth", wrap_pyfunction!(py_dayofmonth, m)?)?;
    m.add("to_degrees", wrap_pyfunction!(py_to_degrees, m)?)?;
    m.add("to_radians", wrap_pyfunction!(py_to_radians, m)?)?;
    m.add("isnull", wrap_pyfunction!(py_isnull, m)?)?;
    m.add("isnotnull", wrap_pyfunction!(py_isnotnull, m)?)?;
    // Phase 15 Batch 2: string (left, right, replace, startswith, endswith, contains, like, ilike, rlike)
    m.add("left", wrap_pyfunction!(py_left, m)?)?;
    m.add("right", wrap_pyfunction!(py_right, m)?)?;
    m.add("replace", wrap_pyfunction!(py_replace, m)?)?;
    m.add("startswith", wrap_pyfunction!(py_startswith, m)?)?;
    m.add("endswith", wrap_pyfunction!(py_endswith, m)?)?;
    m.add("contains", wrap_pyfunction!(py_contains, m)?)?;
    m.add("like", wrap_pyfunction!(py_like, m)?)?;
    m.add("ilike", wrap_pyfunction!(py_ilike, m)?)?;
    m.add("rlike", wrap_pyfunction!(py_rlike, m)?)?;
    // Phase 16: string/regex (regexp_count, regexp_instr, regexp_substr, split_part, find_in_set, format_string, printf)
    m.add("regexp_count", wrap_pyfunction!(py_regexp_count, m)?)?;
    m.add("regexp_instr", wrap_pyfunction!(py_regexp_instr, m)?)?;
    m.add("regexp_substr", wrap_pyfunction!(py_regexp_substr, m)?)?;
    m.add("split_part", wrap_pyfunction!(py_split_part, m)?)?;
    m.add("find_in_set", wrap_pyfunction!(py_find_in_set, m)?)?;
    m.add("format_string", wrap_pyfunction!(py_format_string, m)?)?;
    m.add("printf", wrap_pyfunction!(py_printf, m)?)?;
    // Phase 15 Batch 3: math (cosh, sinh, tanh, acosh, asinh, atanh, cbrt, expm1, log1p, log10, log2, rint, hypot)
    m.add("cosh", wrap_pyfunction!(py_cosh, m)?)?;
    m.add("sinh", wrap_pyfunction!(py_sinh, m)?)?;
    m.add("tanh", wrap_pyfunction!(py_tanh, m)?)?;
    m.add("acosh", wrap_pyfunction!(py_acosh, m)?)?;
    m.add("asinh", wrap_pyfunction!(py_asinh, m)?)?;
    m.add("atanh", wrap_pyfunction!(py_atanh, m)?)?;
    m.add("cbrt", wrap_pyfunction!(py_cbrt, m)?)?;
    m.add("expm1", wrap_pyfunction!(py_expm1, m)?)?;
    m.add("log1p", wrap_pyfunction!(py_log1p, m)?)?;
    m.add("log10", wrap_pyfunction!(py_log10, m)?)?;
    m.add("log2", wrap_pyfunction!(py_log2, m)?)?;
    m.add("rint", wrap_pyfunction!(py_rint, m)?)?;
    m.add("hypot", wrap_pyfunction!(py_hypot, m)?)?;
    Ok(())
}

#[pyfunction]
fn py_col(name: &str) -> PyColumn {
    PyColumn {
        inner: rs_col(name),
    }
}

#[pyfunction]
fn py_lit(value: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyColumn> {
    let inner = if value.is_none() {
        use polars::prelude::*;
        RsColumn::from_expr(lit(NULL), None)
    } else if let Ok(x) = value.extract::<i64>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<f64>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<bool>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<String>() {
        RsColumn::from_expr(polars::prelude::lit(x.as_str()), None)
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "lit() supports only None, int, float, bool, str",
        ));
    };
    Ok(PyColumn { inner })
}

#[pyfunction]
fn py_when(condition: &PyColumn) -> PyWhenBuilder {
    PyWhenBuilder {
        condition: condition.inner.expr().clone(),
    }
}

#[pyfunction]
fn py_coalesce(columns: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    Ok(PyColumn {
        inner: coalesce(&refs),
    })
}

#[pyfunction]
fn py_sum(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_sum(&column.inner),
    }
}

#[pyfunction]
fn py_avg(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: avg(&column.inner),
    }
}

#[pyfunction]
fn py_min(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: min(&column.inner),
    }
}

#[pyfunction]
fn py_max(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: max(&column.inner),
    }
}

#[pyfunction]
fn py_count(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: count(&column.inner),
    }
}

#[pyfunction]
fn py_ascii(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ascii(&column.inner),
    }
}

#[pyfunction]
fn py_format_number(column: &PyColumn, decimals: u32) -> PyColumn {
    PyColumn {
        inner: format_number(&column.inner, decimals),
    }
}

#[pyfunction]
fn py_overlay(column: &PyColumn, replace: &str, pos: i64, length: i64) -> PyColumn {
    PyColumn {
        inner: overlay(&column.inner, replace, pos, length),
    }
}

#[pyfunction]
fn py_position(substr: &str, column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_position(substr, &column.inner),
    }
}

#[pyfunction]
fn py_char(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: crate::functions::char(&column.inner),
    }
}

#[pyfunction]
fn py_chr(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: chr(&column.inner),
    }
}

#[pyfunction]
fn py_base64(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: base64(&column.inner),
    }
}

#[pyfunction]
fn py_unbase64(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: unbase64(&column.inner),
    }
}

#[pyfunction]
fn py_sha1(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: sha1(&column.inner),
    }
}

#[pyfunction]
fn py_sha2(column: &PyColumn, bit_length: i32) -> PyColumn {
    PyColumn {
        inner: sha2(&column.inner, bit_length),
    }
}

#[pyfunction]
fn py_md5(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: md5(&column.inner),
    }
}

#[pyfunction]
fn py_array_compact(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_compact(&column.inner),
    }
}

#[pyfunction]
fn py_array_distinct(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_distinct(&column.inner),
    }
}

#[pyfunction]
fn py_sin(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: sin(&column.inner),
    }
}
#[pyfunction]
fn py_cos(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: cos(&column.inner),
    }
}
#[pyfunction]
fn py_tan(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: tan(&column.inner),
    }
}
#[pyfunction]
fn py_asin(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: asin(&column.inner),
    }
}
#[pyfunction]
fn py_acos(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: acos(&column.inner),
    }
}
#[pyfunction]
fn py_atan(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: atan(&column.inner),
    }
}
#[pyfunction]
fn py_atan2(y: &PyColumn, x: &PyColumn) -> PyColumn {
    PyColumn {
        inner: atan2(&y.inner, &x.inner),
    }
}
#[pyfunction]
fn py_degrees(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: degrees(&column.inner),
    }
}
#[pyfunction]
fn py_radians(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: radians(&column.inner),
    }
}
#[pyfunction]
fn py_signum(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: signum(&column.inner),
    }
}
#[pyfunction]
fn py_quarter(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: quarter(&column.inner),
    }
}
#[pyfunction]
fn py_weekofyear(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: weekofyear(&column.inner),
    }
}
#[pyfunction]
fn py_dayofweek(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: dayofweek(&column.inner),
    }
}
#[pyfunction]
fn py_dayofyear(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: dayofyear(&column.inner),
    }
}
#[pyfunction]
fn py_add_months(column: &PyColumn, n: i32) -> PyColumn {
    PyColumn {
        inner: add_months(&column.inner, n),
    }
}
#[pyfunction]
fn py_months_between(end: &PyColumn, start: &PyColumn) -> PyColumn {
    PyColumn {
        inner: months_between(&end.inner, &start.inner),
    }
}
#[pyfunction]
fn py_next_day(column: &PyColumn, day_of_week: &str) -> PyColumn {
    PyColumn {
        inner: next_day(&column.inner, day_of_week),
    }
}
#[pyfunction]
fn py_cast(column: &PyColumn, type_name: &str) -> PyResult<PyColumn> {
    rs_cast(&column.inner, type_name)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}
#[pyfunction]
fn py_try_cast(column: &PyColumn, type_name: &str) -> PyResult<PyColumn> {
    rs_try_cast(&column.inner, type_name)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}
#[pyfunction]
fn py_isnan(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_isnan(&column.inner),
    }
}
#[pyfunction]
fn py_greatest(columns: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    rs_greatest(&refs)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}
#[pyfunction]
fn py_least(columns: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    rs_least(&refs)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pyfunction]
fn py_nvl(column: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: nvl(&column.inner, &value.inner),
    }
}
#[pyfunction]
fn py_ifnull(column: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ifnull(&column.inner, &value.inner),
    }
}
#[pyfunction]
fn py_nvl2(col1: &PyColumn, col2: &PyColumn, col3: &PyColumn) -> PyColumn {
    PyColumn {
        inner: nvl2(&col1.inner, &col2.inner, &col3.inner),
    }
}
#[pyfunction]
fn py_substr(column: &PyColumn, start: i64, length: Option<i64>) -> PyColumn {
    PyColumn {
        inner: substr(&column.inner, start, length),
    }
}
#[pyfunction]
fn py_power(column: &PyColumn, exp: i64) -> PyColumn {
    PyColumn {
        inner: power(&column.inner, exp),
    }
}
#[pyfunction]
fn py_ln(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ln(&column.inner),
    }
}
#[pyfunction]
fn py_ceiling(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ceiling(&column.inner),
    }
}
#[pyfunction]
fn py_lcase(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: lcase(&column.inner),
    }
}
#[pyfunction]
fn py_ucase(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ucase(&column.inner),
    }
}
#[pyfunction]
fn py_day(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: day(&column.inner),
    }
}
#[pyfunction]
fn py_dayofmonth(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: dayofmonth(&column.inner),
    }
}
#[pyfunction]
fn py_to_degrees(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: to_degrees(&column.inner),
    }
}
#[pyfunction]
fn py_to_radians(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: to_radians(&column.inner),
    }
}
#[pyfunction]
fn py_isnull(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: isnull(&column.inner),
    }
}
#[pyfunction]
fn py_isnotnull(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: isnotnull(&column.inner),
    }
}

#[pyfunction]
fn py_left(column: &PyColumn, n: i64) -> PyColumn {
    PyColumn {
        inner: left(&column.inner, n),
    }
}
#[pyfunction]
fn py_right(column: &PyColumn, n: i64) -> PyColumn {
    PyColumn {
        inner: right(&column.inner, n),
    }
}
#[pyfunction]
fn py_replace(column: &PyColumn, search: &str, replacement: &str) -> PyColumn {
    PyColumn {
        inner: rs_replace(&column.inner, search, replacement),
    }
}
#[pyfunction]
fn py_startswith(column: &PyColumn, prefix: &str) -> PyColumn {
    PyColumn {
        inner: startswith(&column.inner, prefix),
    }
}
#[pyfunction]
fn py_endswith(column: &PyColumn, suffix: &str) -> PyColumn {
    PyColumn {
        inner: endswith(&column.inner, suffix),
    }
}
#[pyfunction]
fn py_contains(column: &PyColumn, substring: &str) -> PyColumn {
    PyColumn {
        inner: contains(&column.inner, substring),
    }
}
#[pyfunction]
fn py_like(column: &PyColumn, pattern: &str) -> PyColumn {
    PyColumn {
        inner: like(&column.inner, pattern),
    }
}
#[pyfunction]
fn py_ilike(column: &PyColumn, pattern: &str) -> PyColumn {
    PyColumn {
        inner: ilike(&column.inner, pattern),
    }
}
#[pyfunction]
fn py_rlike(column: &PyColumn, pattern: &str) -> PyColumn {
    PyColumn {
        inner: rlike(&column.inner, pattern),
    }
}

#[pyfunction]
fn py_regexp_count(column: &PyColumn, pattern: &str) -> PyColumn {
    PyColumn {
        inner: regexp_count(&column.inner, pattern),
    }
}

#[pyfunction]
fn py_regexp_instr(column: &PyColumn, pattern: &str, group_idx: Option<usize>) -> PyColumn {
    PyColumn {
        inner: regexp_instr(&column.inner, pattern, group_idx),
    }
}

#[pyfunction]
fn py_regexp_substr(column: &PyColumn, pattern: &str) -> PyColumn {
    PyColumn {
        inner: regexp_substr(&column.inner, pattern),
    }
}

#[pyfunction]
fn py_split_part(column: &PyColumn, delimiter: &str, part_num: i64) -> PyColumn {
    PyColumn {
        inner: split_part(&column.inner, delimiter, part_num),
    }
}

#[pyfunction]
fn py_find_in_set(str_column: &PyColumn, set_column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: find_in_set(&str_column.inner, &set_column.inner),
    }
}

#[pyfunction]
fn py_format_string(format: &str, columns: Vec<PyRef<PyColumn>>) -> PyColumn {
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    PyColumn {
        inner: format_string(format, &refs),
    }
}

#[pyfunction]
fn py_printf(format: &str, columns: Vec<PyRef<PyColumn>>) -> PyColumn {
    py_format_string(format, columns)
}

#[pyfunction]
fn py_cosh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: cosh(&column.inner),
    }
}
#[pyfunction]
fn py_sinh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: sinh(&column.inner),
    }
}
#[pyfunction]
fn py_tanh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: tanh(&column.inner),
    }
}
#[pyfunction]
fn py_acosh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: acosh(&column.inner),
    }
}
#[pyfunction]
fn py_asinh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: asinh(&column.inner),
    }
}
#[pyfunction]
fn py_atanh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: atanh(&column.inner),
    }
}
#[pyfunction]
fn py_cbrt(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: cbrt(&column.inner),
    }
}
#[pyfunction]
fn py_expm1(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: expm1(&column.inner),
    }
}
#[pyfunction]
fn py_log1p(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: log1p(&column.inner),
    }
}
#[pyfunction]
fn py_log10(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: log10(&column.inner),
    }
}
#[pyfunction]
fn py_log2(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: log2(&column.inner),
    }
}
#[pyfunction]
fn py_rint(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rint(&column.inner),
    }
}
#[pyfunction]
fn py_hypot(x: &PyColumn, y: &PyColumn) -> PyColumn {
    PyColumn {
        inner: hypot(&x.inner, &y.inner),
    }
}

/// Python wrapper for Column (expression).
#[pyclass(name = "Column")]
struct PyColumn {
    inner: RsColumn,
}

#[pymethods]
impl PyColumn {
    fn alias(&self, name: &str) -> Self {
        PyColumn {
            inner: self.inner.alias(name),
        }
    }

    fn is_null(&self) -> Self {
        PyColumn {
            inner: self.inner.is_null(),
        }
    }

    fn is_not_null(&self) -> Self {
        PyColumn {
            inner: self.inner.is_not_null(),
        }
    }

    fn gt(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.gt(other.inner.expr().clone()),
        }
    }

    fn ge(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.gt_eq(other.inner.expr().clone()),
        }
    }

    fn lt(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.lt(other.inner.expr().clone()),
        }
    }

    fn le(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.lt_eq(other.inner.expr().clone()),
        }
    }

    fn eq(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.eq(other.inner.expr().clone()),
        }
    }

    fn ne(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.neq(other.inner.expr().clone()),
        }
    }

    fn and_(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: RsColumn::from_expr(
                self.inner.expr().clone().and(other.inner.expr().clone()),
                None,
            ),
        }
    }

    fn or_(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: RsColumn::from_expr(
                self.inner.expr().clone().or(other.inner.expr().clone()),
                None,
            ),
        }
    }

    fn upper(&self) -> Self {
        PyColumn {
            inner: self.inner.upper(),
        }
    }

    fn lower(&self) -> Self {
        PyColumn {
            inner: self.inner.lower(),
        }
    }

    #[pyo3(signature = (start, length=None))]
    fn substr(&self, start: i64, length: Option<i64>) -> Self {
        PyColumn {
            inner: self.inner.substr(start, length),
        }
    }

    fn ascii_(&self) -> Self {
        PyColumn {
            inner: ascii(&self.inner),
        }
    }

    fn format_number(&self, decimals: u32) -> Self {
        PyColumn {
            inner: format_number(&self.inner, decimals),
        }
    }

    fn overlay(&self, replace: &str, pos: i64, length: i64) -> Self {
        PyColumn {
            inner: overlay(&self.inner, replace, pos, length),
        }
    }

    fn char_(&self) -> Self {
        PyColumn {
            inner: crate::functions::char(&self.inner),
        }
    }

    fn chr_(&self) -> Self {
        PyColumn {
            inner: chr(&self.inner),
        }
    }

    fn base64_(&self) -> Self {
        PyColumn {
            inner: base64(&self.inner),
        }
    }

    fn unbase64_(&self) -> Self {
        PyColumn {
            inner: unbase64(&self.inner),
        }
    }

    fn sha1_(&self) -> Self {
        PyColumn {
            inner: sha1(&self.inner),
        }
    }

    fn sha2_(&self, bit_length: i32) -> Self {
        PyColumn {
            inner: sha2(&self.inner, bit_length),
        }
    }

    fn md5_(&self) -> Self {
        PyColumn {
            inner: md5(&self.inner),
        }
    }

    fn array_compact(&self) -> Self {
        PyColumn {
            inner: array_compact(&self.inner),
        }
    }
    fn array_distinct(&self) -> Self {
        PyColumn {
            inner: array_distinct(&self.inner),
        }
    }

    fn sin(&self) -> Self {
        PyColumn {
            inner: sin(&self.inner),
        }
    }
    fn cos(&self) -> Self {
        PyColumn {
            inner: cos(&self.inner),
        }
    }
    fn tan(&self) -> Self {
        PyColumn {
            inner: tan(&self.inner),
        }
    }
    fn asin_(&self) -> Self {
        PyColumn {
            inner: asin(&self.inner),
        }
    }
    fn acos_(&self) -> Self {
        PyColumn {
            inner: acos(&self.inner),
        }
    }
    fn atan_(&self) -> Self {
        PyColumn {
            inner: atan(&self.inner),
        }
    }
    fn atan2(&self, x: &PyColumn) -> Self {
        PyColumn {
            inner: atan2(&self.inner, &x.inner),
        }
    }
    fn degrees_(&self) -> Self {
        PyColumn {
            inner: degrees(&self.inner),
        }
    }
    fn radians_(&self) -> Self {
        PyColumn {
            inner: radians(&self.inner),
        }
    }
    fn signum(&self) -> Self {
        PyColumn {
            inner: signum(&self.inner),
        }
    }
    fn quarter(&self) -> Self {
        PyColumn {
            inner: quarter(&self.inner),
        }
    }
    fn weekofyear(&self) -> Self {
        PyColumn {
            inner: weekofyear(&self.inner),
        }
    }
    fn dayofweek(&self) -> Self {
        PyColumn {
            inner: dayofweek(&self.inner),
        }
    }
    fn dayofyear(&self) -> Self {
        PyColumn {
            inner: dayofyear(&self.inner),
        }
    }
    fn add_months(&self, n: i32) -> Self {
        PyColumn {
            inner: add_months(&self.inner, n),
        }
    }
    fn months_between(&self, start: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.months_between(&start.inner),
        }
    }
    fn next_day(&self, day_of_week: &str) -> Self {
        PyColumn {
            inner: next_day(&self.inner, day_of_week),
        }
    }
    fn cast(&self, type_name: &str) -> PyResult<Self> {
        rs_cast(&self.inner, type_name)
            .map(|inner| PyColumn { inner })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }
    fn try_cast(&self, type_name: &str) -> PyResult<Self> {
        rs_try_cast(&self.inner, type_name)
            .map(|inner| PyColumn { inner })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }
    fn isnan(&self) -> Self {
        PyColumn {
            inner: rs_isnan(&self.inner),
        }
    }
    fn nvl(&self, value: &PyColumn) -> Self {
        PyColumn {
            inner: nvl(&self.inner, &value.inner),
        }
    }
    fn ifnull(&self, value: &PyColumn) -> Self {
        PyColumn {
            inner: ifnull(&self.inner, &value.inner),
        }
    }
    fn power(&self, exp: i64) -> Self {
        PyColumn {
            inner: power(&self.inner, exp),
        }
    }
    fn ln(&self) -> Self {
        PyColumn {
            inner: ln(&self.inner),
        }
    }
    fn ceiling(&self) -> Self {
        PyColumn {
            inner: ceiling(&self.inner),
        }
    }
    fn lcase(&self) -> Self {
        PyColumn {
            inner: lcase(&self.inner),
        }
    }
    fn ucase(&self) -> Self {
        PyColumn {
            inner: ucase(&self.inner),
        }
    }
    fn day(&self) -> Self {
        PyColumn {
            inner: day(&self.inner),
        }
    }
    fn dayofmonth(&self) -> Self {
        PyColumn {
            inner: dayofmonth(&self.inner),
        }
    }
    fn to_degrees(&self) -> Self {
        PyColumn {
            inner: to_degrees(&self.inner),
        }
    }
    fn to_radians(&self) -> Self {
        PyColumn {
            inner: to_radians(&self.inner),
        }
    }
    fn isnull(&self) -> Self {
        PyColumn {
            inner: isnull(&self.inner),
        }
    }
    fn isnotnull(&self) -> Self {
        PyColumn {
            inner: isnotnull(&self.inner),
        }
    }
    fn left(&self, n: i64) -> Self {
        PyColumn {
            inner: left(&self.inner, n),
        }
    }
    fn right(&self, n: i64) -> Self {
        PyColumn {
            inner: right(&self.inner, n),
        }
    }
    fn replace(&self, search: &str, replacement: &str) -> Self {
        PyColumn {
            inner: rs_replace(&self.inner, search, replacement),
        }
    }
    fn startswith(&self, prefix: &str) -> Self {
        PyColumn {
            inner: startswith(&self.inner, prefix),
        }
    }
    fn endswith(&self, suffix: &str) -> Self {
        PyColumn {
            inner: endswith(&self.inner, suffix),
        }
    }
    fn contains(&self, substring: &str) -> Self {
        PyColumn {
            inner: contains(&self.inner, substring),
        }
    }
    fn like(&self, pattern: &str) -> Self {
        PyColumn {
            inner: like(&self.inner, pattern),
        }
    }
    fn ilike(&self, pattern: &str) -> Self {
        PyColumn {
            inner: ilike(&self.inner, pattern),
        }
    }
    fn rlike(&self, pattern: &str) -> Self {
        PyColumn {
            inner: rlike(&self.inner, pattern),
        }
    }
    fn cosh(&self) -> Self {
        PyColumn {
            inner: cosh(&self.inner),
        }
    }
    fn sinh(&self) -> Self {
        PyColumn {
            inner: sinh(&self.inner),
        }
    }
    fn tanh(&self) -> Self {
        PyColumn {
            inner: tanh(&self.inner),
        }
    }
    fn acosh(&self) -> Self {
        PyColumn {
            inner: acosh(&self.inner),
        }
    }
    fn asinh(&self) -> Self {
        PyColumn {
            inner: asinh(&self.inner),
        }
    }
    fn atanh_(&self) -> Self {
        PyColumn {
            inner: atanh(&self.inner),
        }
    }
    fn cbrt(&self) -> Self {
        PyColumn {
            inner: cbrt(&self.inner),
        }
    }
    fn expm1(&self) -> Self {
        PyColumn {
            inner: expm1(&self.inner),
        }
    }
    fn log1p(&self) -> Self {
        PyColumn {
            inner: log1p(&self.inner),
        }
    }
    fn log10(&self) -> Self {
        PyColumn {
            inner: log10(&self.inner),
        }
    }
    fn log2(&self) -> Self {
        PyColumn {
            inner: log2(&self.inner),
        }
    }
    fn rint(&self) -> Self {
        PyColumn {
            inner: rint(&self.inner),
        }
    }
    fn hypot(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: hypot(&self.inner, &other.inner),
        }
    }

    /// Array/list size (PySpark size).
    fn size(&self) -> Self {
        PyColumn {
            inner: self.inner.array_size(),
        }
    }

    /// Element at 1-based index (PySpark element_at).
    fn element_at(&self, index: i64) -> Self {
        PyColumn {
            inner: self.inner.element_at(index),
        }
    }

    /// Explode list into one row per element (PySpark explode).
    fn explode(&self) -> Self {
        PyColumn {
            inner: self.inner.explode(),
        }
    }

    /// 1-based index of first occurrence of value in list, or 0 if not found (PySpark array_position).
    fn array_position(&self, value: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.array_position(value.inner.expr().clone()),
        }
    }

    /// New list with all elements equal to value removed (PySpark array_remove).
    fn array_remove(&self, value: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.array_remove(value.inner.expr().clone()),
        }
    }

    /// Repeat each element n times (PySpark array_repeat). Not implemented.
    fn array_repeat(&self, n: i64) -> Self {
        PyColumn {
            inner: self.inner.array_repeat(n),
        }
    }

    /// Explode list with position (PySpark posexplode). Returns (pos_column, value_column).
    fn posexplode(&self) -> (Self, Self) {
        let (pos, val) = self.inner.posexplode();
        (PyColumn { inner: pos }, PyColumn { inner: val })
    }

    /// First value in partition (PySpark first_value). Use with .over().
    fn first_value(&self) -> Self {
        PyColumn {
            inner: self.inner.first_value(),
        }
    }

    /// Last value in partition (PySpark last_value). Use with .over().
    fn last_value(&self) -> Self {
        PyColumn {
            inner: self.inner.last_value(),
        }
    }

    /// Percent rank in partition. Window is applied; pass partition_by.
    fn percent_rank(&self, partition_by: Vec<String>, descending: bool) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.percent_rank(&refs, descending),
        }
    }

    /// Cumulative distribution in partition. Window is applied; pass partition_by.
    fn cume_dist(&self, partition_by: Vec<String>, descending: bool) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.cume_dist(&refs, descending),
        }
    }

    /// Ntile: bucket 1..n by rank within partition. Window is applied; pass partition_by.
    fn ntile(&self, n: u32, partition_by: Vec<String>, descending: bool) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.ntile(n, &refs, descending),
        }
    }

    /// Nth value in partition by order (1-based n). Window is applied; pass partition_by, do not call .over() again.
    fn nth_value(&self, n: i64, partition_by: Vec<String>, descending: bool) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.nth_value(n, &refs, descending),
        }
    }

    /// Check if string matches regex (PySpark regexp_like).
    fn regexp_like(&self, pattern: &str) -> Self {
        PyColumn {
            inner: self.inner.regexp_like(pattern),
        }
    }

    /// Count of non-overlapping regex matches (PySpark regexp_count).
    fn regexp_count(&self, pattern: &str) -> Self {
        PyColumn {
            inner: regexp_count(&self.inner, pattern),
        }
    }

    /// 1-based position of first regex match (PySpark regexp_instr).
    fn regexp_instr(&self, pattern: &str, group_idx: Option<usize>) -> Self {
        PyColumn {
            inner: regexp_instr(&self.inner, pattern, group_idx),
        }
    }

    /// First substring matching regex (PySpark regexp_substr).
    fn regexp_substr(&self, pattern: &str) -> Self {
        PyColumn {
            inner: regexp_substr(&self.inner, pattern),
        }
    }

    /// Split by delimiter and return 1-based part (PySpark split_part).
    fn split_part(&self, delimiter: &str, part_num: i64) -> Self {
        PyColumn {
            inner: split_part(&self.inner, delimiter, part_num),
        }
    }

    /// 1-based index in comma-delimited set (PySpark find_in_set).
    fn find_in_set(&self, set_column: &PyColumn) -> Self {
        PyColumn {
            inner: find_in_set(&self.inner, &set_column.inner),
        }
    }
}

/// Python wrapper for WhenBuilder (when(cond).then(val).otherwise(val)).
#[pyclass(name = "WhenBuilder")]
struct PyWhenBuilder {
    condition: Expr,
}

#[pymethods]
impl PyWhenBuilder {
    fn then(&self, value: &PyColumn) -> PyThenBuilder {
        let when_then =
            polars::prelude::when(self.condition.clone()).then(value.inner.expr().clone());
        PyThenBuilder { when_then }
    }
}

/// Python wrapper for ThenBuilder (.otherwise(val)).
#[pyclass(name = "ThenBuilder")]
struct PyThenBuilder {
    when_then: polars::prelude::Then,
}

#[pymethods]
impl PyThenBuilder {
    fn otherwise(&self, value: &PyColumn) -> PyColumn {
        let expr = self.when_then.clone().otherwise(value.inner.expr().clone());
        PyColumn {
            inner: RsColumn::from_expr(expr, None),
        }
    }
}

/// Python wrapper for SparkSession.
#[pyclass(name = "SparkSession")]
struct PySparkSession {
    inner: SparkSession,
}

#[pymethods]
impl PySparkSession {
    #[new]
    fn new() -> Self {
        PySparkSession {
            inner: SparkSession::new(None, None, std::collections::HashMap::new()),
        }
    }

    /// Create a SparkSession via builder().app_name(...).get_or_create()
    #[classmethod]
    fn builder(_cls: &Bound<'_, pyo3::types::PyType>) -> PyResult<PySparkSessionBuilder> {
        Ok(PySparkSessionBuilder {
            app_name: None,
            master: None,
            config: std::collections::HashMap::new(),
        })
    }

    fn is_case_sensitive(&self) -> bool {
        self.inner.is_case_sensitive()
    }

    /// Create a DataFrame from a list of 3-tuples (id, age, name) and column names.
    /// data: list of (int, int, str), column_names: list of 3 strings e.g. ["id", "age", "name"].
    fn create_dataframe(
        &self,
        _py: Python<'_>,
        data: &Bound<'_, pyo3::types::PyAny>,
        column_names: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        let data_rust: Vec<(i64, i64, String)> = data
            .extract()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let names_ref: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .create_dataframe(data_rust, names_ref)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_csv(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_csv(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_parquet(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_json(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_json(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "sql")]
    fn create_or_replace_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner
            .create_or_replace_temp_view(name, df.inner.clone());
    }

    #[cfg(feature = "sql")]
    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .table(name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "sql")]
    fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sql(query)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "delta")]
    fn read_delta(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_delta(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "delta")]
    fn read_delta_version(&self, path: &str, version: Option<i64>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_delta_with_version(Path::new(path), version)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for SparkSessionBuilder.
#[pyclass(name = "SparkSessionBuilder")]
struct PySparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: std::collections::HashMap<String, String>,
}

#[pymethods]
impl PySparkSessionBuilder {
    fn app_name<'a>(mut slf: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        slf.app_name = Some(name.to_string());
        slf
    }

    fn master<'a>(mut slf: PyRefMut<'a, Self>, master: &str) -> PyRefMut<'a, Self> {
        slf.master = Some(master.to_string());
        slf
    }

    fn config<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        slf.config.insert(key.to_string(), value.to_string());
        slf
    }

    fn get_or_create(slf: PyRef<'_, Self>) -> PySparkSession {
        let mut config = std::collections::HashMap::new();
        for (k, v) in &slf.config {
            config.insert(k.clone(), v.clone());
        }
        let inner = SparkSession::new(slf.app_name.clone(), slf.master.clone(), config);
        PySparkSession { inner }
    }
}

/// Python wrapper for DataFrame.
#[pyclass(name = "DataFrame")]
struct PyDataFrame {
    inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    fn count(&self) -> PyResult<usize> {
        self.inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    #[pyo3(signature = (n=None))]
    fn show(&self, n: Option<usize>) -> PyResult<()> {
        self.inner
            .show(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    #[cfg(feature = "delta")]
    fn write_delta(&self, path: &str, overwrite: bool) -> PyResult<()> {
        self.inner
            .write_delta(Path::new(path), overwrite)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Collect as list of dicts (column name -> value per row).
    fn collect(&self, py: Python<'_>) -> PyResult<PyObject> {
        let pl_df = self
            .inner
            .collect()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let df = pl_df.as_ref();
        let names = df.get_column_names();
        let nrows = df.height();
        let rows = pyo3::types::PyList::empty(py);
        for i in 0..nrows {
            let row_dict = PyDict::new(py);
            for (col_idx, name) in names.iter().enumerate() {
                let s = df.get_columns().get(col_idx).ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("column index out of range")
                })?;
                let av = s
                    .get(i)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let py_val = any_value_to_py(py, av)?;
                row_dict.set_item(name.as_str(), py_val)?;
            }
            rows.append(&*row_dict)?;
        }
        Ok(rows.into())
    }

    fn filter(&self, condition: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .filter(condition.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn select(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .select(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_column(&self, column_name: &str, expr: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .with_column(column_name, expr.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (cols, ascending=None))]
    fn order_by(&self, cols: Vec<String>, ascending: Option<Vec<bool>>) -> PyResult<PyDataFrame> {
        let asc = ascending.unwrap_or_else(|| vec![true; cols.len()]);
        let df = self
            .inner
            .order_by(cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(), asc)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn group_by(&self, cols: Vec<String>) -> PyResult<PyGroupedData> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let gd = self
            .inner
            .group_by(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyGroupedData { inner: gd })
    }

    #[pyo3(signature = (other, on, how="inner"))]
    fn join(&self, other: &PyDataFrame, on: Vec<String>, how: &str) -> PyResult<PyDataFrame> {
        let join_type = match how.to_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "outer" => JoinType::Outer,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "how must be 'inner', 'left', 'right', or 'outer'",
                ));
            }
        };
        let on_refs: Vec<&str> = on.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .join(&other.inner, on_refs, join_type)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn union(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .union(&other.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn union_by_name(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .union_by_name(&other.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (subset=None))]
    fn distinct(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .inner
            .distinct(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn drop(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .drop(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (subset=None))]
    fn dropna(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .inner
            .dropna(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn fillna(&self, value: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .fillna(value.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .limit(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_column_renamed(&self, old: &str, new: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .with_column_renamed(old, new)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    // Phase 12: sample, first, head, tail, take, is_empty, to_json, explain, print_schema, checkpoint, repartition, coalesce, offset
    #[pyo3(signature = (with_replacement=false, fraction=1.0, seed=None))]
    fn sample(
        &self,
        with_replacement: bool,
        fraction: f64,
        seed: Option<u64>,
    ) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sample(with_replacement, fraction, seed)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn first(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .first()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn head(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .head(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn tail(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .tail(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn take(&self, n: usize) -> PyResult<PyDataFrame> {
        self.head(n)
    }

    fn is_empty(&self) -> PyResult<bool> {
        Ok(self.inner.is_empty())
    }

    fn to_json(&self) -> PyResult<Vec<String>> {
        self.inner
            .to_json()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    fn explain(&self) -> PyResult<String> {
        Ok(self.inner.explain())
    }

    fn print_schema(&self) -> PyResult<String> {
        self.inner
            .print_schema()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    fn checkpoint(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .checkpoint()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn local_checkpoint(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .local_checkpoint()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn repartition(&self, num_partitions: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .repartition(num_partitions)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn coalesce(&self, num_partitions: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .coalesce(num_partitions)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn offset(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .offset(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn random_split(&self, weights: Vec<f64>, seed: Option<u64>) -> PyResult<Vec<PyDataFrame>> {
        let dfs = self
            .inner
            .random_split(&weights, seed)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(dfs
            .into_iter()
            .map(|df| PyDataFrame { inner: df })
            .collect())
    }

    fn summary(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .summary()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn to_df(&self, names: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .to_df(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn select_expr(&self, exprs: Vec<String>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .select_expr(&exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn col_regex(&self, pattern: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .col_regex(pattern)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_columns(&self, mapping: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyDataFrame> {
        let mut exprs: Vec<(String, Expr)> = Vec::new();
        if let Ok(dict) = mapping.downcast::<PyDict>() {
            for (k, v) in dict.iter() {
                let name: String = k.extract()?;
                let col: PyRef<PyColumn> = v.extract()?;
                exprs.push((name, col.inner.expr().clone()));
            }
        } else if let Ok(list) = mapping.downcast::<pyo3::types::PyList>() {
            for item in list.iter() {
                let tuple: (String, PyRef<PyColumn>) = item.extract()?;
                exprs.push((tuple.0, tuple.1.inner.expr().clone()));
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "with_columns expects dict[str, Column] or list[tuple[str, Column]]",
            ));
        }
        let df = self
            .inner
            .with_columns(&exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_columns_renamed(
        &self,
        mapping: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        let mut renames: Vec<(String, String)> = Vec::new();
        if let Ok(dict) = mapping.downcast::<PyDict>() {
            for (k, v) in dict.iter() {
                let old_name: String = k.extract()?;
                let new_name: String = v.extract()?;
                renames.push((old_name, new_name));
            }
        } else if let Ok(list) = mapping.downcast::<pyo3::types::PyList>() {
            for item in list.iter() {
                let tuple: (String, String) = item.extract()?;
                renames.push(tuple);
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "with_columns_renamed expects dict[str, str] or list[tuple[str, str]]",
            ));
        }
        let df = self
            .inner
            .with_columns_renamed(&renames)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn stat(&self) -> PyDataFrameStat {
        PyDataFrameStat {
            df: self.inner.clone(),
        }
    }

    fn na(&self) -> PyDataFrameNa {
        PyDataFrameNa {
            df: self.inner.clone(),
        }
    }

    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.collect(py)
    }
}

/// Python wrapper for DataFrame.stat() (cov, corr).
#[pyclass(name = "DataFrameStat")]
struct PyDataFrameStat {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrameStat {
    fn cov(&self, col1: &str, col2: &str) -> PyResult<f64> {
        self.df
            .stat()
            .cov(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    fn corr(&self, col1: &str, col2: &str) -> PyResult<f64> {
        self.df
            .stat()
            .corr(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }
}

/// Python wrapper for DataFrame.na() (fill, drop).
#[pyclass(name = "DataFrameNa")]
struct PyDataFrameNa {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrameNa {
    fn fill(&self, value: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .df
            .na()
            .fill(value.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (subset=None))]
    fn drop(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .df
            .na()
            .drop(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for GroupedData.
#[pyclass(name = "GroupedData")]
struct PyGroupedData {
    inner: GroupedData,
}

#[pymethods]
impl PyGroupedData {
    fn count(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn sum(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sum(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn avg(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .avg(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(name = "min")]
    fn min_(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .min(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn max(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .max(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn agg(&self, exprs: Vec<PyRef<PyColumn>>) -> PyResult<PyDataFrame> {
        let aggregations: Vec<Expr> = exprs.iter().map(|c| c.inner.expr().clone()).collect();
        let df = self
            .inner
            .agg(aggregations)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Convert Polars AnyValue to a Python object.
fn any_value_to_py(py: Python<'_>, av: polars::prelude::AnyValue<'_>) -> PyResult<PyObject> {
    use polars::prelude::AnyValue;
    use pyo3::conversion::IntoPyObjectExt;
    match av {
        AnyValue::Null => py.None().into_bound_py_any(py).map(Into::into),
        AnyValue::Boolean(b) => b.into_bound_py_any(py).map(Into::into),
        AnyValue::Int32(i) => i.into_bound_py_any(py).map(Into::into),
        AnyValue::Int64(i) => i.into_bound_py_any(py).map(Into::into),
        AnyValue::UInt32(u) => u.into_bound_py_any(py).map(Into::into),
        AnyValue::UInt64(u) => u.into_bound_py_any(py).map(Into::into),
        AnyValue::Float32(f) => f.into_bound_py_any(py).map(Into::into),
        AnyValue::Float64(f) => f.into_bound_py_any(py).map(Into::into),
        AnyValue::String(s) => s.to_string().into_bound_py_any(py).map(Into::into),
        AnyValue::StringOwned(s) => s.to_string().into_bound_py_any(py).map(Into::into),
        other => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "unsupported type for collect: {:?}",
            other
        ))),
    }
}
