//! Python Column type (PySpark sql Column).
#![allow(non_snake_case)] // PySpark param names (dayOfWeek) for API parity

use crate::column::Column as RsColumn;
use crate::functions::isnan as rs_isnan;
use crate::functions::{
    acos, acosh, add_months, array_append, array_compact, array_distinct, array_except,
    array_insert, array_intersect, array_prepend, array_union, ascii, asin, asinh, atan, atan2,
    atanh, base64, bit_length, cast as rs_cast, cbrt, ceiling, chr, contains, cos, cosh,
    date_from_unix_date, day, dayofmonth, dayofweek, dayofyear, degrees, endswith, expm1,
    factorial, find_in_set, format_number, from_csv, from_unixtime, get, get_json_object, hypot,
    ifnull, ilike, isin_i64, isin_str, isnotnull, isnull, json_tuple, lcase, left, like, ln, log10,
    log1p, log2, map_concat, map_contains_key, map_filter_value_gt, map_from_entries,
    map_zip_with_coalesce, md5, month, next_day, nullif, nvl, overlay, pmod, power, quarter,
    radians, regexp_count, regexp_extract, regexp_instr, regexp_replace, regexp_substr,
    replace as rs_replace, reverse, right, rint, rlike, sha1, sha2, signum, sin, sinh, split,
    split_part, startswith, tan, tanh, timestamp_micros, timestamp_millis, timestamp_seconds,
    to_degrees, to_radians, try_add, try_cast as rs_try_cast, try_divide, try_multiply,
    try_subtract, typeof_, ucase, unbase64, unix_date, unix_timestamp, weekofyear, year,
    zip_with_coalesce,
};
use crate::functions::{
    crc32, exp, floor, initcap, length, levenshtein, ltrim, repeat, round, rtrim, trim, xxhash64,
};
use crate::functions::{schema_of_csv, schema_of_json, to_csv};
use polars::prelude::{lit, NULL};
use pyo3::prelude::*;

use super::order::PySortOrder;

/// Convert a Python value to RsColumn (for operator overloads). Accepts Column or scalar (int, float, bool, str, None).
fn py_any_to_column(value: &Bound<'_, pyo3::types::PyAny>) -> PyResult<RsColumn> {
    if let Ok(pycol) = value.downcast::<PyColumn>() {
        return Ok(pycol.borrow().inner.clone());
    }
    if value.is_none() {
        return Ok(RsColumn::from_expr(lit(NULL), None));
    }
    if let Ok(x) = value.extract::<i64>() {
        return Ok(RsColumn::from_expr(lit(x), None));
    }
    if let Ok(x) = value.extract::<f64>() {
        return Ok(RsColumn::from_expr(lit(x), None));
    }
    if let Ok(x) = value.extract::<bool>() {
        return Ok(RsColumn::from_expr(lit(x), None));
    }
    if let Ok(x) = value.extract::<String>() {
        return Ok(RsColumn::from_expr(lit(x.as_str()), None));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "arithmetic operands must be Column or scalar (int, float, bool, str, None)",
    ))
}

/// Python wrapper for Column (expression).
/// Expression representing a column or computed value for use in DataFrame operations.
///
/// Create with ``col("name")`` or ``lit(value)``. Chain methods for expressions (e.g. ``col("a").alias("x")``).
#[pyclass(name = "Column")]
pub struct PyColumn {
    pub inner: RsColumn,
}

#[pymethods]
impl PyColumn {
    /// Return a Column with the same values but a different name (e.g. for select/agg output).
    ///
    /// Args:
    ///     name: New display/schema name for this expression.
    ///
    /// Returns:
    ///     Column: Same expression with the given alias.
    fn alias(&self, name: &str) -> Self {
        PyColumn {
            inner: self.inner.alias(name),
        }
    }

    /// True where this column is null.
    fn is_null(&self) -> Self {
        PyColumn {
            inner: self.inner.is_null(),
        }
    }

    /// True where this column is not null.
    fn is_not_null(&self) -> Self {
        PyColumn {
            inner: self.inner.is_not_null(),
        }
    }

    /// True where this column's value is in the given list (PySpark isin). Empty list yields false for all rows.
    ///
    /// Args:
    ///     values: List of int or str; empty list is supported (filter returns 0 rows).
    fn isin(&self, values: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let list = values.downcast::<pyo3::types::PyList>()?;
        let len = list.len();
        if len == 0 {
            return Ok(PyColumn {
                inner: isin_i64(&self.inner, &[]),
            });
        }
        let mut ints: Vec<i64> = Vec::with_capacity(len);
        for item in list.iter() {
            if let Ok(i) = item.extract::<i64>() {
                ints.push(i);
            } else {
                break;
            }
        }
        if ints.len() == len {
            return Ok(PyColumn {
                inner: isin_i64(&self.inner, &ints),
            });
        }
        let mut strs: Vec<String> = Vec::with_capacity(len);
        for item in list.iter() {
            strs.push(item.extract::<String>()?);
        }
        let refs: Vec<&str> = strs.iter().map(|s| s.as_str()).collect();
        Ok(PyColumn {
            inner: isin_str(&self.inner, &refs),
        })
    }

    /// Ascending sort order (nulls first). For use in order_by_exprs().
    fn asc(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc(),
        }
    }

    /// Ascending sort order with nulls first.
    fn asc_nulls_first(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc_nulls_first(),
        }
    }

    /// Ascending sort order with nulls last.
    fn asc_nulls_last(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc_nulls_last(),
        }
    }

    /// Descending sort order (nulls last). For use in order_by_exprs().
    fn desc(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc(),
        }
    }

    /// Descending sort order with nulls first.
    fn desc_nulls_first(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc_nulls_first(),
        }
    }

    /// Descending sort order with nulls last.
    fn desc_nulls_last(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc_nulls_last(),
        }
    }

    /// Greater than (self > other). Accepts Column or scalar (int, float, bool, str, None).
    fn gt(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.gt(other_col.into_expr()),
        })
    }

    /// Greater than or equal (self >= other). Accepts Column or scalar.
    fn ge(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.gt_eq(other_col.into_expr()),
        })
    }

    /// Less than (self < other). Accepts Column or scalar.
    fn lt(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.lt(other_col.into_expr()),
        })
    }

    /// Less than or equal (self <= other). Accepts Column or scalar.
    fn le(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.lt_eq(other_col.into_expr()),
        })
    }

    /// Equal (self == other). Accepts Column or scalar.
    fn eq(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.eq(other_col.into_expr()),
        })
    }

    /// Not equal (self != other). Accepts Column or scalar.
    fn ne(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.neq(other_col.into_expr()),
        })
    }

    /// Null-safe equality (PySpark eqNullSafe): true if both null or both equal. Accepts Column or scalar.
    fn eq_null_safe(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.eq_null_safe(&other_col),
        })
    }

    /// Inclusive range (PySpark between): true where (self >= low) and (self <= high). Accepts Column or scalar.
    fn between(
        &self,
        low: &Bound<'_, pyo3::types::PyAny>,
        high: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<Self> {
        let low_col = py_any_to_column(low)?;
        let high_col = py_any_to_column(high)?;
        let ge = self.inner.expr().clone().gt_eq(low_col.expr().clone());
        let le = self.inner.expr().clone().lt_eq(high_col.expr().clone());
        Ok(PyColumn {
            inner: RsColumn::from_expr(ge.and(le), None),
        })
    }

    /// Python > operator. Enables col("a") > col("b") and col("a") > 5.
    fn __gt__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        self.gt(other)
    }

    /// Python >= operator.
    fn __ge__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        self.ge(other)
    }

    /// Python < operator.
    fn __lt__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        self.lt(other)
    }

    /// Python <= operator.
    fn __le__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        self.le(other)
    }

    /// Python == operator.
    fn __eq__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        self.eq(other)
    }

    /// Python != operator.
    fn __ne__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        self.ne(other)
    }

    /// Logical AND with another boolean column. Also supports Python & operator.
    fn __and__(&self, other: &PyColumn) -> PyResult<Self> {
        Ok(self.and_(other))
    }

    /// Logical OR with another boolean column. Also supports Python | operator.
    fn __or__(&self, other: &PyColumn) -> PyResult<Self> {
        Ok(self.or_(other))
    }

    /// Add (self + other). PySpark: col + col or col + scalar. Accepts Column or int/float/bool/str/None.
    fn __add__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.add_pyspark(&other_col),
        })
    }

    /// Reflected add (other + self). Enables scalar + col (e.g. 2 + col("x")).
    fn __radd__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: other_col.add_pyspark(&self.inner),
        })
    }

    /// Subtract (self - other). PySpark: col - col or col - scalar.
    fn __sub__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.subtract_pyspark(&other_col),
        })
    }

    /// Reflected subtract (other - self). Enables scalar - col.
    fn __rsub__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: other_col.subtract_pyspark(&self.inner),
        })
    }

    /// Multiply (self * other). PySpark: col * col or col * scalar (e.g. col("a") * 2).
    fn __mul__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.multiply_pyspark(&other_col),
        })
    }

    /// Reflected multiply (other * self). Enables scalar * col (e.g. 3 * col("x")).
    fn __rmul__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: other_col.multiply_pyspark(&self.inner),
        })
    }

    /// True division (self / other). PySpark: col / col or col / scalar.
    fn __truediv__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.divide_pyspark(&other_col),
        })
    }

    /// Reflected true division (other / self).
    fn __rtruediv__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: other_col.divide_pyspark(&self.inner),
        })
    }

    /// Modulo (self % other). PySpark: col % col or col % scalar.
    fn __mod__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.mod_pyspark(&other_col),
        })
    }

    /// Reflected modulo (other % self).
    fn __rmod__(&self, other: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: other_col.mod_pyspark(&self.inner),
        })
    }

    /// Logical AND with another boolean column.
    fn and_(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: RsColumn::from_expr(
                self.inner.expr().clone().and(other.inner.expr().clone()),
                None,
            ),
        }
    }

    /// Logical OR with another boolean column.
    fn or_(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: RsColumn::from_expr(
                self.inner.expr().clone().or(other.inner.expr().clone()),
                None,
            ),
        }
    }

    /// Uppercase string values.
    fn upper(&self) -> Self {
        PyColumn {
            inner: self.inner.upper(),
        }
    }

    /// Lowercase string values.
    fn lower(&self) -> Self {
        PyColumn {
            inner: self.inner.lower(),
        }
    }

    /// Substring from start (1-based); optional length.
    #[pyo3(signature = (start, length=None))]
    fn substr(&self, start: i64, length: Option<i64>) -> Self {
        PyColumn {
            inner: self.inner.substr(start, length),
        }
    }

    /// ASCII code of first character (Spark ascii).
    fn ascii_(&self) -> Self {
        PyColumn {
            inner: ascii(&self.inner),
        }
    }

    /// Format numeric column with fixed decimal places.
    fn format_number(&self, decimals: u32) -> Self {
        PyColumn {
            inner: format_number(&self.inner, decimals),
        }
    }

    /// Overlay: replace length chars at pos with replace string.
    fn overlay(&self, replace: &str, pos: i64, length: i64) -> Self {
        PyColumn {
            inner: overlay(&self.inner, replace, pos, length),
        }
    }

    /// Spark char: single character from numeric code.
    fn char_(&self) -> Self {
        PyColumn {
            inner: crate::functions::char(&self.inner),
        }
    }

    /// Chr: character from numeric code (alias for char).
    fn chr_(&self) -> Self {
        PyColumn {
            inner: chr(&self.inner),
        }
    }

    /// Base64 encode string values.
    fn base64_(&self) -> Self {
        PyColumn {
            inner: base64(&self.inner),
        }
    }

    /// Base64 decode string values.
    fn unbase64_(&self) -> Self {
        PyColumn {
            inner: unbase64(&self.inner),
        }
    }

    /// SHA-1 hash of string values.
    fn sha1_(&self) -> Self {
        PyColumn {
            inner: sha1(&self.inner),
        }
    }

    /// SHA-2 hash; bit_length 224, 256, 384, or 512.
    fn sha2_(&self, bit_length: i32) -> Self {
        PyColumn {
            inner: sha2(&self.inner, bit_length),
        }
    }

    /// MD5 hash of string values.
    fn md5_(&self) -> Self {
        PyColumn {
            inner: md5(&self.inner),
        }
    }

    /// Remove nulls from array column.
    fn array_compact(&self) -> Self {
        PyColumn {
            inner: array_compact(&self.inner),
        }
    }
    /// Distinct elements in array.
    fn array_distinct(&self) -> Self {
        PyColumn {
            inner: array_distinct(&self.inner),
        }
    }

    /// Append element to array.
    fn array_append(&self, elem: &PyColumn) -> Self {
        PyColumn {
            inner: array_append(&self.inner, &elem.inner),
        }
    }

    /// Prepend element to array.
    fn array_prepend(&self, elem: &PyColumn) -> Self {
        PyColumn {
            inner: array_prepend(&self.inner, &elem.inner),
        }
    }

    /// Insert element into array at position.
    fn array_insert(&self, pos: &PyColumn, elem: &PyColumn) -> Self {
        PyColumn {
            inner: array_insert(&self.inner, &pos.inner, &elem.inner),
        }
    }

    /// Array elements in self but not in other.
    fn array_except(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: array_except(&self.inner, &other.inner),
        }
    }

    /// Intersection of two array columns.
    fn array_intersect(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: array_intersect(&self.inner, &other.inner),
        }
    }

    /// Union of two array columns (distinct).
    fn array_union(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: array_union(&self.inner, &other.inner),
        }
    }

    /// Concatenate two map columns.
    fn map_concat(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: map_concat(&self.inner, &other.inner),
        }
    }

    /// Filter map entries by value > threshold (internal utility).
    #[pyo3(name = "_map_filter_value_gt")]
    fn map_filter_value_gt(&self, threshold: f64) -> Self {
        PyColumn {
            inner: map_filter_value_gt(&self.inner, threshold),
        }
    }

    /// Zip two arrays with coalesce for length mismatch (internal utility).
    #[pyo3(name = "_zip_with_coalesce")]
    fn zip_with_coalesce(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: zip_with_coalesce(&self.inner, &other.inner),
        }
    }

    /// Zip two maps with coalesce (internal utility).
    #[pyo3(name = "_map_zip_with_coalesce")]
    fn map_zip_with_coalesce(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: map_zip_with_coalesce(&self.inner, &other.inner),
        }
    }

    /// Build map from array of (key, value) entries.
    fn map_from_entries(&self) -> Self {
        PyColumn {
            inner: map_from_entries(&self.inner),
        }
    }

    /// True if map contains key.
    fn map_contains_key(&self, key: &PyColumn) -> Self {
        PyColumn {
            inner: map_contains_key(&self.inner, &key.inner),
        }
    }

    /// Get value from map/struct by key.
    fn get(&self, key: &PyColumn) -> Self {
        PyColumn {
            inner: get(&self.inner, &key.inner),
        }
    }

    /// Division that returns null on divide-by-zero.
    fn try_divide(&self, right: &PyColumn) -> Self {
        PyColumn {
            inner: try_divide(&self.inner, &right.inner),
        }
    }

    /// Addition that returns null on overflow.
    fn try_add(&self, right: &PyColumn) -> Self {
        PyColumn {
            inner: try_add(&self.inner, &right.inner),
        }
    }

    /// Subtraction that returns null on overflow.
    fn try_subtract(&self, right: &PyColumn) -> Self {
        PyColumn {
            inner: try_subtract(&self.inner, &right.inner),
        }
    }

    /// Multiplication that returns null on overflow.
    fn try_multiply(&self, right: &PyColumn) -> Self {
        PyColumn {
            inner: try_multiply(&self.inner, &right.inner),
        }
    }

    /// Multiply by another column or literal (PySpark multiply). Broadcasts scalars.
    fn multiply(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.multiply(&other.inner),
        }
    }

    /// Bit length of string (bytes * 8).
    fn bit_length(&self) -> Self {
        PyColumn {
            inner: bit_length(&self.inner),
        }
    }

    /// Type name of each value as string.
    fn typeof_(&self) -> Self {
        PyColumn {
            inner: typeof_(&self.inner),
        }
    }

    /// Sine (radians).
    fn sin(&self) -> Self {
        PyColumn {
            inner: sin(&self.inner),
        }
    }
    /// Cosine (radians).
    fn cos(&self) -> Self {
        PyColumn {
            inner: cos(&self.inner),
        }
    }
    /// Tangent (radians).
    fn tan(&self) -> Self {
        PyColumn {
            inner: tan(&self.inner),
        }
    }
    /// Arc sine.
    fn asin_(&self) -> Self {
        PyColumn {
            inner: asin(&self.inner),
        }
    }
    /// Arc cosine.
    fn acos_(&self) -> Self {
        PyColumn {
            inner: acos(&self.inner),
        }
    }
    /// Arc tangent.
    fn atan_(&self) -> Self {
        PyColumn {
            inner: atan(&self.inner),
        }
    }
    /// Two-argument arc tangent (y, x).
    fn atan2(&self, x: &PyColumn) -> Self {
        PyColumn {
            inner: atan2(&self.inner, &x.inner),
        }
    }
    /// Convert radians to degrees.
    fn degrees_(&self) -> Self {
        PyColumn {
            inner: degrees(&self.inner),
        }
    }
    /// Convert degrees to radians.
    fn radians_(&self) -> Self {
        PyColumn {
            inner: radians(&self.inner),
        }
    }
    /// Sign of number (-1, 0, or 1).
    fn signum(&self) -> Self {
        PyColumn {
            inner: signum(&self.inner),
        }
    }
    /// Quarter of date (1-4).
    fn quarter(&self) -> Self {
        PyColumn {
            inner: quarter(&self.inner),
        }
    }
    /// Week of year (1-53).
    fn weekofyear(&self) -> Self {
        PyColumn {
            inner: weekofyear(&self.inner),
        }
    }
    /// Day of week (e.g. 1=Sunday).
    fn dayofweek(&self) -> Self {
        PyColumn {
            inner: dayofweek(&self.inner),
        }
    }
    /// Day of year (1-366).
    fn dayofyear(&self) -> Self {
        PyColumn {
            inner: dayofyear(&self.inner),
        }
    }
    /// Add n months to date/timestamp.
    fn add_months(&self, n: i32) -> Self {
        PyColumn {
            inner: add_months(&self.inner, n),
        }
    }
    /// Months between two dates.
    fn months_between(&self, start: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.months_between(&start.inner, true),
        }
    }
    /// Next date that is the given day of week.
    #[pyo3(signature = (dayOfWeek))]
    fn next_day(&self, dayOfWeek: &str) -> Self {
        PyColumn {
            inner: next_day(&self.inner, dayOfWeek),
        }
    }
    /// Cast the column to the given type. Invalid values cause an error at execution.
    ///
    /// Args:
    ///     type_name: Target type string (e.g. "int", "long", "double", "string", "boolean", "date", "timestamp").
    ///
    /// Returns:
    ///     Column: Expression that evaluates to the cast type.
    ///
    /// Raises:
    ///     ValueError: If the type name is not supported or cast is invalid.
    fn cast(&self, type_name: &str) -> PyResult<Self> {
        rs_cast(&self.inner, type_name)
            .map(|inner| PyColumn { inner })
            .map_err(pyo3::exceptions::PyValueError::new_err)
    }
    /// Cast the column to the given type; invalid values become null instead of raising.
    ///
    /// Args:
    ///     type_name: Target type string (same as ``cast()``).
    ///
    /// Returns:
    ///     Column: Expression that evaluates to the cast type or null where conversion fails.
    ///
    /// Raises:
    ///     ValueError: If the type name is not supported.
    fn try_cast(&self, type_name: &str) -> PyResult<Self> {
        rs_try_cast(&self.inner, type_name)
            .map(|inner| PyColumn { inner })
            .map_err(pyo3::exceptions::PyValueError::new_err)
    }
    /// PySpark alias for cast. Cast the column to the given type.
    fn astype(&self, type_name: &str) -> PyResult<Self> {
        self.cast(type_name)
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
    /// Extract year from date/timestamp column (PySpark year).
    fn year(&self) -> Self {
        PyColumn {
            inner: year(&self.inner),
        }
    }
    /// Extract month from date/timestamp column (PySpark month).
    fn month(&self) -> Self {
        PyColumn {
            inner: month(&self.inner),
        }
    }
    /// Return null if self equals other, else self (PySpark nullif).
    fn nullif(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: nullif(&self.inner, &other.inner),
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
    fn length(&self) -> Self {
        PyColumn {
            inner: length(&self.inner),
        }
    }
    fn trim(&self) -> Self {
        PyColumn {
            inner: trim(&self.inner),
        }
    }
    fn ltrim(&self) -> Self {
        PyColumn {
            inner: ltrim(&self.inner),
        }
    }
    fn rtrim(&self) -> Self {
        PyColumn {
            inner: rtrim(&self.inner),
        }
    }
    #[pyo3(name = "repeat")]
    fn repeat_(&self, n: i32) -> Self {
        PyColumn {
            inner: repeat(&self.inner, n),
        }
    }
    fn reverse(&self) -> Self {
        PyColumn {
            inner: reverse(&self.inner),
        }
    }
    fn initcap(&self) -> Self {
        PyColumn {
            inner: initcap(&self.inner),
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
            inner: like(&self.inner, pattern, None),
        }
    }
    fn ilike(&self, pattern: &str) -> Self {
        PyColumn {
            inner: ilike(&self.inner, pattern, None),
        }
    }
    fn rlike(&self, pattern: &str) -> Self {
        PyColumn {
            inner: rlike(&self.inner, pattern),
        }
    }
    #[pyo3(signature = (pattern, idx=0))]
    fn regexp_extract(&self, pattern: &str, idx: usize) -> Self {
        PyColumn {
            inner: regexp_extract(&self.inner, pattern, idx),
        }
    }
    fn regexp_replace(&self, pattern: &str, replacement: &str) -> Self {
        PyColumn {
            inner: regexp_replace(&self.inner, pattern, replacement),
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
    fn floor(&self) -> Self {
        PyColumn {
            inner: floor(&self.inner),
        }
    }
    #[pyo3(name = "round", signature = (scale=0))]
    fn round_(&self, scale: u32) -> Self {
        PyColumn {
            inner: round(&self.inner, scale),
        }
    }
    #[pyo3(name = "exp")]
    fn exp_(&self) -> Self {
        PyColumn {
            inner: exp(&self.inner),
        }
    }
    fn levenshtein(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: levenshtein(&self.inner, &other.inner),
        }
    }
    #[pyo3(name = "crc32")]
    fn crc32_(&self) -> Self {
        PyColumn {
            inner: crc32(&self.inner),
        }
    }
    #[pyo3(name = "xxhash64")]
    fn xxhash64_(&self) -> Self {
        PyColumn {
            inner: xxhash64(&self.inner),
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

    // --- Window functions (use with .over(partition_by); Fixes #187) ---

    /// Apply window partitioning. Use after row_number(), rank(), dense_rank(), lag(), lead(), first_value(), last_value(), or aggregation (e.g. sum).
    /// Example: ``col("salary").row_number(True).over(["dept"])`` or ``sum(col("amount")).over(["id"])``.
    #[pyo3(signature = (partition_by))]
    fn over(&self, partition_by: Vec<String>) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.over(&refs),
        }
    }

    /// Row number (1, 2, 3) by this column's order within partition. Use with .over(partition_by).
    #[pyo3(signature = (descending=false))]
    fn row_number(&self, descending: bool) -> Self {
        PyColumn {
            inner: self.inner.row_number(descending),
        }
    }

    /// Rank (ties same rank, gaps). Use with .over(partition_by).
    #[pyo3(signature = (descending=false))]
    fn rank(&self, descending: bool) -> Self {
        PyColumn {
            inner: self.inner.rank(descending),
        }
    }

    /// Dense rank (no gaps). Use with .over(partition_by).
    #[pyo3(signature = (descending=false))]
    fn dense_rank(&self, descending: bool) -> Self {
        PyColumn {
            inner: self.inner.dense_rank(descending),
        }
    }

    /// Lag: value from n rows before in partition. Use with .over(partition_by).
    #[pyo3(signature = (n=1))]
    fn lag(&self, n: i64) -> Self {
        PyColumn {
            inner: self.inner.lag(n),
        }
    }

    /// Lead: value from n rows after in partition. Use with .over(partition_by).
    #[pyo3(signature = (n=1))]
    fn lead(&self, n: i64) -> Self {
        PyColumn {
            inner: self.inner.lead(n),
        }
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

    /// Split string by delimiter into list of strings (PySpark split). Optional limit: at most that many parts.
    #[pyo3(signature = (delimiter, limit=None))]
    fn split(&self, delimiter: &str, limit: Option<i32>) -> Self {
        PyColumn {
            inner: split(&self.inner, delimiter, limit),
        }
    }
    /// Split by delimiter and return 1-based part (PySpark split_part).
    fn split_part(&self, delimiter: &str, part_num: i64) -> Self {
        PyColumn {
            inner: split_part(&self.inner, delimiter, part_num),
        }
    }

    /// 1-based index in comma-delimited set (PySpark find_in_set).
    fn find_in_set(&self, set_col: &PyColumn) -> Self {
        PyColumn {
            inner: find_in_set(&self.inner, &set_col.inner),
        }
    }
    /// Extract JSON path from string column (PySpark get_json_object).
    fn get_json_object(&self, path: &str) -> Self {
        PyColumn {
            inner: get_json_object(&self.inner, path),
        }
    }
    /// Extract keys from JSON as struct (PySpark json_tuple). Returns struct with one field per key.
    #[pyo3(signature = (*fields))]
    fn json_tuple(&self, fields: Vec<String>) -> Self {
        let key_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: json_tuple(&self.inner, &key_refs),
        }
    }

    /// Parse CSV string to struct (PySpark from_csv). Minimal: split by comma.
    #[allow(clippy::wrong_self_convention)] // PySpark Column.from_csv(column)
    fn from_csv(&self) -> Self {
        PyColumn {
            inner: from_csv(&self.inner),
        }
    }

    /// Format struct as CSV string (PySpark to_csv). Minimal.
    fn to_csv(&self) -> Self {
        PyColumn {
            inner: to_csv(&self.inner),
        }
    }

    /// Schema of CSV string (PySpark schema_of_csv). Returns literal schema string; minimal stub.
    fn schema_of_csv(&self) -> Self {
        PyColumn {
            inner: schema_of_csv(&self.inner),
        }
    }

    /// Schema of JSON string (PySpark schema_of_json). Returns literal schema string; minimal stub.
    fn schema_of_json(&self) -> Self {
        PyColumn {
            inner: schema_of_json(&self.inner),
        }
    }

    fn unix_timestamp(&self, format: Option<&str>) -> Self {
        PyColumn {
            inner: unix_timestamp(&self.inner, format),
        }
    }
    #[allow(clippy::wrong_self_convention)] // PySpark Column.from_unixtime(column, format)
    fn from_unixtime(&self, format: Option<&str>) -> Self {
        PyColumn {
            inner: from_unixtime(&self.inner, format),
        }
    }
    fn timestamp_seconds(&self) -> Self {
        PyColumn {
            inner: timestamp_seconds(&self.inner),
        }
    }
    fn timestamp_millis(&self) -> Self {
        PyColumn {
            inner: timestamp_millis(&self.inner),
        }
    }
    fn timestamp_micros(&self) -> Self {
        PyColumn {
            inner: timestamp_micros(&self.inner),
        }
    }
    fn unix_date(&self) -> Self {
        PyColumn {
            inner: unix_date(&self.inner),
        }
    }
    fn date_from_unix_date(&self) -> Self {
        PyColumn {
            inner: date_from_unix_date(&self.inner),
        }
    }
    fn pmod(&self, divisor: &PyColumn) -> Self {
        PyColumn {
            inner: pmod(&self.inner, &divisor.inner),
        }
    }
    fn factorial(&self) -> Self {
        PyColumn {
            inner: factorial(&self.inner),
        }
    }
}
