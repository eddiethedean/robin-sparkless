use polars::prelude::*;

/// Comparison operators of interest for PySpark-style coercion.
///
/// We keep a local alias to avoid leaking polars::prelude in public signatures unnecessarily.
pub type CompareOp = polars::prelude::Operator;

/// Type precedence for ANSI SQL type coercion
/// Higher precedence types can be coerced to, lower precedence types are coerced from
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)] // Decimal reserved for future use
enum TypePrecedence {
    Int = 1,
    Long = 2,
    Decimal = 3,
    Float = 4,
    Double = 5,
    String = 6,
}

/// Convert Polars DataType to TypePrecedence
fn dtype_to_precedence(dtype: &DataType) -> Option<TypePrecedence> {
    match dtype {
        DataType::Int32 => Some(TypePrecedence::Int),
        DataType::Int64 => Some(TypePrecedence::Long),
        DataType::Float32 => Some(TypePrecedence::Float),
        DataType::Float64 => Some(TypePrecedence::Double),
        DataType::String => Some(TypePrecedence::String),
        // Decimal: add when Polars exposes Decimal in public API / dtype set we use
        _ => None,
    }
}

/// Determine the common type for two columns based on PySpark's type precedence rules
/// Returns the tightest (highest precedence) common type that both can be coerced to.
/// #681: Unknown dtype is treated as String so join/union with Int64 vs Unknown coerce to String (PySpark parity).
pub fn find_common_type(left: &DataType, right: &DataType) -> Result<DataType, PolarsError> {
    let left_norm = if matches!(left, DataType::Unknown(_)) {
        DataType::String
    } else {
        left.clone()
    };
    let right_norm = if matches!(right, DataType::Unknown(_)) {
        DataType::String
    } else {
        right.clone()
    };
    let left = &left_norm;
    let right = &right_norm;
    let left_prec = dtype_to_precedence(left);
    let right_prec = dtype_to_precedence(right);

    match (left_prec, right_prec) {
        (Some(l), Some(r)) => {
            // Return the type with higher precedence
            let target_prec = if l > r { l } else { r };
            match target_prec {
                TypePrecedence::Int => Ok(DataType::Int32),
                TypePrecedence::Long => Ok(DataType::Int64),
                TypePrecedence::Float => Ok(DataType::Float32),
                TypePrecedence::Double => Ok(DataType::Float64),
                TypePrecedence::String => Ok(DataType::String),
                _ => Err(PolarsError::ComputeError(
                    format!(
                        "Type coercion: unsupported type precedence {target_prec:?}. Supported: Int32, Int64, Float32, Float64, String."
                    )
                    .into(),
                )),
            }
        }
        _ => {
            // If types don't match known precedence, try to find a common type
            if is_numeric(left) && is_numeric(right) {
                Ok(DataType::Float64)
            } else if left == right {
                Ok(left.clone())
            } else if left == &DataType::String || right == &DataType::String {
                // #613: unionByName string vs numeric -> coerce to String (PySpark parity)
                Ok(DataType::String)
            } else {
                Err(PolarsError::ComputeError(
                    format!(
                        "Type coercion: cannot find common type for {left:?} and {right:?}. Hint: use cast() to align types, or ensure both are numeric or both are string."
                    )
                    .into(),
                ))
            }
        }
    }
}

/// Common type for join keys (#850). When one side is numeric and the other is String, prefer the
/// numeric type so the result schema has a numeric key column (PySpark parity).
pub fn find_common_type_for_join(
    left: &DataType,
    right: &DataType,
) -> Result<DataType, PolarsError> {
    let left_norm = if matches!(left, DataType::Unknown(_)) {
        DataType::String
    } else {
        left.clone()
    };
    let right_norm = if matches!(right, DataType::Unknown(_)) {
        DataType::String
    } else {
        right.clone()
    };
    let left = &left_norm;
    let right = &right_norm;
    // Prefer numeric when one is numeric and one is string (PySpark result schema has numeric key).
    if is_numeric(left) && right == &DataType::String {
        return Ok(left.clone());
    }
    if left == &DataType::String && is_numeric(right) {
        return Ok(right.clone());
    }
    find_common_type(left, right)
}

/// Build (left_expr, right_expr) that cast two columns to a common type and alias to the same name.
/// Use in join key coercion and union_by_name when both sides have the column but types differ.
pub fn coerce_expr_pair(
    left_name: &str,
    right_name: &str,
    left_dtype: &DataType,
    right_dtype: &DataType,
    alias: &str,
) -> Result<(Expr, Expr), PolarsError> {
    let common = find_common_type(left_dtype, right_dtype)?;
    Ok((
        col(left_name).cast(common.clone()).alias(alias),
        col(right_name).cast(common).alias(alias),
    ))
}

/// Like coerce_expr_pair but uses find_common_type_for_join so join result key is numeric when one side is numeric (#850).
pub fn coerce_expr_pair_for_join(
    left_name: &str,
    right_name: &str,
    left_dtype: &DataType,
    right_dtype: &DataType,
    alias: &str,
) -> Result<(Expr, Expr), PolarsError> {
    let common = find_common_type_for_join(left_dtype, right_dtype)?;
    Ok((
        col(left_name).cast(common.clone()).alias(alias),
        col(right_name).cast(common).alias(alias),
    ))
}

/// Check if a DataType is numeric
fn is_numeric(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Public wrapper for use in DataFrame coercion (#974).
pub fn is_numeric_public(dtype: &DataType) -> bool {
    is_numeric(dtype)
}

/// Check if a DataType is date/datetime (temporal types that we can cast from string via try_cast).
fn is_date_or_datetime(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Date | DataType::Datetime(_, _))
}

/// Coerce a column expression to a target type
pub fn coerce_to_type(expr: Expr, target_type: DataType) -> Expr {
    expr.cast(target_type)
}

/// Coerce two expressions to their common type for comparison
pub fn coerce_for_comparison(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
) -> Result<(Expr, Expr), PolarsError> {
    if left_type == right_type {
        // Same type, no coercion needed
        return Ok((left, right));
    }

    let common_type = find_common_type(left_type, right_type)?;

    let left_coerced = if left_type == &common_type {
        left
    } else {
        coerce_to_type(left, common_type.clone())
    };

    let right_coerced = if right_type == &common_type {
        right
    } else {
        coerce_to_type(right, common_type)
    };

    Ok((left_coerced, right_coerced))
}

/// Coerce two expressions for PySpark-style comparison semantics.
///
/// This extends [`coerce_for_comparison`] to handle string–numeric combinations by
/// parsing string values to numbers (double) instead of erroring, mirroring PySpark:
///
/// - String values that parse as numbers (e.g. "123", " 45.6 ") are compared numerically.
/// - Non‑numeric strings behave as null under numeric comparison (non-matching in filters).
///
/// For plain numeric–numeric inputs, it delegates to [`coerce_for_comparison`].
pub fn coerce_for_pyspark_comparison(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
    _op: &CompareOp,
) -> Result<(Expr, Expr), PolarsError> {
    use crate::column::Column;

    // Fast-path: both numeric -> existing numeric coercion.
    if is_numeric(left_type) && is_numeric(right_type) {
        return coerce_for_comparison(left, right, left_type, right_type);
    }

    // Helper to wrap an Expr in try_to_number (double) semantics when it represents a value
    // that should be interpreted as numeric if possible.
    fn wrap_try_to_number(expr: Expr) -> Result<Expr, PolarsError> {
        let col = Column::from_expr(expr, None);
        let coerced = crate::functions::try_to_number(&col, None)
            .map_err(|e| PolarsError::ComputeError(e.into()))?;
        Ok(coerced.into_expr())
    }

    // String–numeric (or numeric–string): route string side through try_to_number and
    // cast numeric side to Float64 so both sides line up.
    let string_numeric = (left_type == &DataType::String && is_numeric(right_type))
        || (right_type == &DataType::String && is_numeric(left_type));

    if string_numeric {
        let left_out = if left_type == &DataType::String {
            wrap_try_to_number(left)?
        } else if is_numeric(left_type) {
            coerce_to_type(left, DataType::Float64)
        } else {
            left
        };

        let right_out = if right_type == &DataType::String {
            wrap_try_to_number(right)?
        } else if is_numeric(right_type) {
            coerce_to_type(right, DataType::Float64)
        } else {
            right
        };

        return Ok((left_out, right_out));
    }

    // Date/datetime vs string: cast string side to the temporal type (PySpark implicit cast).
    // #1084: For literals use direct cast so filter(col("dt") >= "2024-01-12 00:00:00") works
    // when the plan sends string literals; try_cast uses map() which may not handle literals.
    fn wrap_try_to_temporal(expr: Expr, target: &DataType) -> Result<Expr, PolarsError> {
        if matches!(target, DataType::Date | DataType::Datetime(_, _)) {
            if matches!(&expr, Expr::Literal(_)) {
                return Ok(expr.cast(target.clone()));
            }
        }
        let col = Column::from_expr(expr, None);
        let type_name = match target {
            DataType::Date => "date",
            DataType::Datetime(..) => "timestamp",
            _ => {
                return Err(PolarsError::ComputeError(
                    "date or datetime type required".to_string().into(),
                ));
            }
        };
        let coerced = crate::functions::try_cast(&col, type_name)
            .map_err(|e| PolarsError::ComputeError(e.into()))?;
        Ok(coerced.into_expr())
    }

    let temporal_string = (is_date_or_datetime(left_type) && right_type == &DataType::String)
        || (left_type == &DataType::String && is_date_or_datetime(right_type));

    if temporal_string {
        let left_out = if left_type == &DataType::String {
            wrap_try_to_temporal(left, right_type)?
        } else {
            left
        };
        let right_out = if right_type == &DataType::String {
            wrap_try_to_temporal(right, left_type)?
        } else {
            right
        };
        return Ok((left_out, right_out));
    }

    // #615: Date vs datetime comparison (e.g. datetime_col < date_col): cast Date to Datetime
    // so both sides are comparable (PySpark treats date as start-of-day timestamp).
    let date_vs_datetime = (left_type == &DataType::Date
        && matches!(right_type, DataType::Datetime(_, _)))
        || (matches!(left_type, DataType::Datetime(_, _)) && right_type == &DataType::Date);
    if date_vs_datetime {
        let target_dt = if matches!(left_type, DataType::Datetime(_, _)) {
            left_type.clone()
        } else {
            right_type.clone()
        };
        let left_out = if left_type == &DataType::Date {
            coerce_to_type(left, target_dt.clone())
        } else {
            left
        };
        let right_out = if right_type == &DataType::Date {
            coerce_to_type(right, target_dt)
        } else {
            right
        };
        return Ok((left_out, right_out));
    }

    // Equal non-numeric types: leave as-is for now.
    if left_type == right_type && !is_numeric(left_type) {
        return Ok((left, right));
    }

    // Fallback to generic comparison coercion (may error with a clear message).
    coerce_for_comparison(left, right, left_type, right_type)
}

/// Infer DataType from an expression when it is a literal (for coercion heuristics).
pub fn infer_type_from_expr(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Literal(lv) => {
            let dt = lv.get_datatype();
            Some(if matches!(dt, DataType::Unknown(_)) {
                DataType::Float64
            } else {
                dt
            })
        }
        _ => None,
    }
}

/// True when the expression is a string literal whose text does *not* parse as a number.
fn is_non_numeric_string_literal(expr: &Expr) -> bool {
    // Current Polars `LiteralValue` does not expose underlying string contents in a way
    // that lets us cheaply distinguish "numeric-looking" strings from arbitrary text.
    // For eqNullSafe parity, it is acceptable (and desirable for #1471) to treat all
    // string literals as numeric candidates and let `try_to_number` decide:
    //
    // - Numeric strings like "123" are parsed and compared numerically.
    // - Non-numeric strings like "abc" become null and simply don't match.
    //
    // So we never classify a literal as "non-numeric string" here.
    let _ = expr;
    false
}

/// Coerce left/right for eq_null_safe so string–numeric compares like PySpark (try_to_number on string side).
/// Infers types from literals; assumes String for column (so string–numeric gets coerced).
/// When both sides are column refs (no type info), cast both to string so string vs numeric compares (e.g. "100" and 100).
pub fn coerce_for_pyspark_eq_null_safe(
    left: Expr,
    right: Expr,
) -> Result<(Expr, Expr), PolarsError> {
    // If either side is a null literal, leave expressions as-is.
    // Comparing any dtype to a null literal is well-defined, and attempting string/numeric
    // coercion here can introduce invalid comparisons (e.g. string vs i64) in Polars planning.
    if matches!(infer_type_from_expr(&left), Some(DataType::Null))
        || matches!(infer_type_from_expr(&right), Some(DataType::Null))
    {
        return Ok((left, right));
    }

    let left_inferred = infer_type_from_expr(&left);
    let right_inferred = infer_type_from_expr(&right);
    let left_non_numeric_str_lit = is_non_numeric_string_literal(&left);
    let right_non_numeric_str_lit = is_non_numeric_string_literal(&right);

    // Derive comparison dtypes with a bit of asymmetry to mirror PySpark-style coercion:
    // - When only one side is a literal string and the other is a column/expr, treat the
    //   non-literal side as numeric (Float64) so string‑numeric coercion (try_to_number)
    //   kicks in for cases like int_col <=> "123" (issue #1471).
    // - When only one side has a non-string literal type, treat the unknown side as String
    //   so string‑numeric detection in coerce_for_pyspark_comparison still works when the
    //   real column type is discovered later.
    let (left_ty, right_ty) = match (left_inferred.clone(), right_inferred.clone()) {
        // Both sides known: respect their literal dtypes.
        (Some(lt), Some(rt)) => (lt, rt),
        // String literal vs unknown expression:
        // - If the unknown side is a *column* expr, keep both as String so
        //   string-column vs string-literal behaves like string equality with
        //   null-safe semantics (issue #260).
        // - Otherwise (e.g. string literal vs numeric literal), treat the unknown
        //   side as numeric (Float64) so string–numeric coercion kicks in (#1471).
        (Some(DataType::String), None) if matches!(right, Expr::Column(_)) => {
            (DataType::String, DataType::String)
        }
        (None, Some(DataType::String)) if matches!(left, Expr::Column(_)) => {
            (DataType::String, DataType::String)
        }
        (Some(DataType::String), None) => (DataType::String, DataType::Float64),
        (None, Some(DataType::String)) => (DataType::Float64, DataType::String),
        // Non-string literal vs unknown expression: treat unknown as String so that
        // generic comparison coercion (including string–numeric) still applies.
        (Some(lt), None) => (lt, DataType::String),
        (None, Some(rt)) => (DataType::String, rt),
        (None, None) => (DataType::String, DataType::String),
    };

    // Both unknown (column refs): cast both to string so string vs numeric doesn't error (PySpark coercion).
    if left_inferred.is_none() && right_inferred.is_none() {
        let left_str = left.cast(DataType::String);
        let right_str = right.cast(DataType::String);
        return Ok((left_str, right_str));
    }

    coerce_for_pyspark_comparison(left, right, &left_ty, &right_ty, &CompareOp::Eq)
}

/// #974: Coerce string–numeric for arithmetic (+, -, *, /, %, **). String side is cast via try_to_number to Float64.
pub fn coerce_for_pyspark_arithmetic(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
) -> Result<(Expr, Expr), PolarsError> {
    use crate::column::Column;
    fn wrap_try_to_number(expr: Expr) -> Result<Expr, PolarsError> {
        let col = Column::from_expr(expr, None);
        let coerced = crate::functions::try_to_number(&col, None)
            .map_err(|e| PolarsError::ComputeError(e.into()))?;
        Ok(coerced.into_expr())
    }
    let string_numeric = (left_type == &DataType::String && is_numeric(right_type))
        || (right_type == &DataType::String && is_numeric(left_type));
    if !string_numeric {
        return Ok((left, right));
    }
    let left_out = if left_type == &DataType::String {
        wrap_try_to_number(left)?
    } else if is_numeric(left_type) {
        coerce_to_type(left, DataType::Float64)
    } else {
        left
    };
    let right_out = if right_type == &DataType::String {
        wrap_try_to_number(right)?
    } else if is_numeric(right_type) {
        coerce_to_type(right, DataType::Float64)
    } else {
        right
    };
    Ok((left_out, right_out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{IntoLazy, df};

    #[test]
    fn numeric_numeric_uses_standard_coercion() -> Result<(), PolarsError> {
        let df = df!(
            "a" => &[1i32, 2, 3],
            "b" => &[1i64, 2, 3]
        )?;

        let a = col("a");
        let b = col("b");
        let (ac, bc) = coerce_for_pyspark_comparison(
            a.clone(),
            b.clone(),
            &DataType::Int32,
            &DataType::Int64,
            &CompareOp::Eq,
        )?;

        // After coercion both sides should be comparable without error and all rows match.
        let out = df.lazy().filter(ac.eq(bc)).collect()?;
        assert_eq!(out.height(), 3);
        Ok(())
    }

    #[test]
    fn string_numeric_uses_try_to_number() -> Result<(), PolarsError> {
        let df = df!(
            "s" => &["123", " 45.5 ", "abc"],
            "n" => &[123i32, 46, 0]
        )?;

        let s_expr = col("s");
        let n_expr = col("n");

        let (s_coerced, n_coerced) = coerce_for_pyspark_comparison(
            s_expr.clone(),
            n_expr.clone(),
            &DataType::String,
            &DataType::Int32,
            &CompareOp::Eq,
        )?;

        let out = df.lazy().filter(s_coerced.eq(n_coerced)).collect()?;

        // Only the first row matches ("123" == 123); " 45.5 " != 46, "abc" -> null (non-match).
        assert_eq!(out.height(), 1);
        Ok(())
    }

    /// #615: datetime_col < date_col must return rows (PySpark: date as start-of-day for comparison).
    #[test]
    fn date_datetime_comparison_coerces_date_to_datetime() -> Result<(), PolarsError> {
        use chrono::{NaiveDate, NaiveDateTime};
        use polars::prelude::*;

        let ts = NaiveDateTime::parse_from_str("2024-01-14 23:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let df = df!(
            "ts_col" => [ts],
            "date_col" => [dt]
        )?;
        let df = df
            .lazy()
            .with_columns([
                col("ts_col").cast(DataType::Datetime(TimeUnit::Microseconds, None)),
                col("date_col").cast(DataType::Date),
            ])
            .collect()?;
        let lf = df.lazy();

        let ts_expr = col("ts_col");
        let date_expr = col("date_col");
        let (ts_c, date_c) = coerce_for_pyspark_comparison(
            ts_expr,
            date_expr,
            &DataType::Datetime(TimeUnit::Microseconds, None),
            &DataType::Date,
            &CompareOp::Lt,
        )?;

        let out = lf.filter(ts_c.lt(date_c)).collect()?;
        assert_eq!(
            out.height(),
            1,
            "#615: datetime < date should return one row"
        );
        Ok(())
    }

    #[test]
    fn eq_null_safe_int_column_vs_string_literal_coerces() -> Result<(), PolarsError> {
        use polars::prelude::df;

        let df = df!(
            "val" => &[Some(123i64), Some(456i64), None],
        )?;
        let lf = df.lazy();

        let col_expr = col("val");
        let lit_expr = lit("123");

        let (left_c, right_c) = coerce_for_pyspark_eq_null_safe(col_expr, lit_expr)?;
        let out = lf
            .with_column(left_c.eq(right_c).alias("match"))
            .collect()?;

        let matches = out.column("match")?.bool()?;
        let got: Vec<Option<bool>> = matches.into_iter().collect();
        assert_eq!(got, vec![Some(true), Some(false), Some(false)]);

        Ok(())
    }
}
