//! Complex UDF implementations.

use super::*;

/// Build map (list of structs {key, value}) from two list columns. PySpark map_from_arrays.
pub fn apply_map_from_arrays(
    columns: &mut [Column],
    disable_map_key_normalization: bool,
) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::StructChunked;
    use polars::chunked_array::builder::get_list_builder;
    use polars::datatypes::Field;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "map_from_arrays needs two columns (keys, values)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let keys_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let values_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let keys_ca = keys_series
        .list()
        .map_err(|e| compute_err("map_from_arrays keys", e))?;
    let values_ca = values_series
        .list()
        .map_err(|e| compute_err("map_from_arrays values", e))?;
    let key_dtype = keys_ca.inner_dtype().clone();
    let value_dtype = values_ca.inner_dtype().clone();
    let struct_dtype = DataType::Struct(vec![
        Field::new("key".into(), key_dtype),
        Field::new("value".into(), value_dtype),
    ]);
    let mut builder = get_list_builder(&struct_dtype, 64, keys_ca.len(), name.as_str().into());
    for (opt_k, opt_v) in keys_ca.amortized_iter().zip(values_ca.amortized_iter()) {
        match (opt_k, opt_v) {
            (Some(k_amort), Some(v_amort)) => {
                let k_list = k_amort.as_ref().as_list();
                let v_list = v_amort.as_ref().as_list();
                let mut row_structs: Vec<Series> = Vec::new();
                for (opt_ke, opt_ve) in k_list.amortized_iter().zip(v_list.amortized_iter()) {
                    if let (Some(ke), Some(ve)) = (opt_ke, opt_ve) {
                        let ke_s = normalize_map_key_series(
                            ke.deep_clone(),
                            disable_map_key_normalization,
                        )?;
                        let ve_s = ve.deep_clone();
                        let len = ke_s.len();
                        let fields: [&Series; 2] = [&ke_s, &ve_s];
                        let st = StructChunked::from_series(
                            PlSmallStr::EMPTY,
                            len,
                            fields.iter().copied(),
                        )
                        .map_err(|e| compute_err("struct", e))?
                        .into_series();
                        row_structs.push(st);
                    }
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &struct_dtype))
                        .map_err(|e| compute_err("builder", e))?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder
                        .append_series(&combined)
                        .map_err(|e| compute_err("builder", e))?;
                }
            }
            _ => {
                builder.append_null();
            }
        }
    }
    let out = builder.finish().into_series();
    Ok(Some(Column::new(name, out)))
}

/// Zip two array columns into List(Struct{left, right}) for zip_with. Shorter padded with null.
pub fn apply_zip_arrays_to_struct(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::StructChunked;
    use polars::chunked_array::builder::get_list_builder;
    use polars::datatypes::Field;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "zip_arrays_to_struct needs two columns (left, right)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| compute_err("zip_with left", e))?;
    let b_ca = b_series
        .list()
        .map_err(|e| compute_err("zip_with right", e))?;
    let left_dtype = a_ca.inner_dtype().clone();
    let right_dtype = b_ca.inner_dtype().clone();
    let struct_dtype = DataType::Struct(vec![
        Field::new("left".into(), left_dtype.clone()),
        Field::new("right".into(), right_dtype.clone()),
    ]);
    let mut builder = get_list_builder(&struct_dtype, 64, a_ca.len(), name.as_str().into());
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        match (opt_a, opt_b) {
            (Some(a_amort), Some(b_amort)) => {
                let a_list = a_amort.as_ref().as_list();
                let b_list = b_amort.as_ref().as_list();
                let a_elems: Vec<Series> = a_list
                    .amortized_iter()
                    .flatten()
                    .map(|amort| amort.deep_clone())
                    .collect();
                let b_elems: Vec<Series> = b_list
                    .amortized_iter()
                    .flatten()
                    .map(|amort| amort.deep_clone())
                    .collect();
                let max_len = a_elems.len().max(b_elems.len());
                let mut row_structs: Vec<Series> = Vec::new();
                for i in 0..max_len {
                    let left_s = a_elems.get(i).cloned();
                    let right_s = b_elems.get(i).cloned();
                    let (mut left_series, mut right_series) = match (left_s, right_s) {
                        (Some(l), Some(r)) => (l, r),
                        (Some(l), None) => {
                            let r = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &right_dtype,
                                false,
                            )
                            .map_err(|e| compute_err("zip null", e))?;
                            (l, r)
                        }
                        (None, Some(r)) => {
                            let l = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &left_dtype,
                                false,
                            )
                            .map_err(|e| compute_err("zip null", e))?;
                            (l, r)
                        }
                        (None, None) => {
                            let mut l = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &left_dtype,
                                false,
                            )
                            .map_err(|e| compute_err("zip null", e))?;
                            l.rename("left".into());
                            let mut r = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &right_dtype,
                                false,
                            )
                            .map_err(|e| compute_err("zip null", e))?;
                            r.rename("right".into());
                            (l, r)
                        }
                    };
                    left_series.rename("left".into());
                    right_series.rename("right".into());
                    let len = left_series.len();
                    let fields: [&Series; 2] = [&left_series, &right_series];
                    let st =
                        StructChunked::from_series(PlSmallStr::EMPTY, len, fields.iter().copied())
                            .map_err(|e| compute_err("zip struct", e))?
                            .into_series();
                    row_structs.push(st);
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &struct_dtype))
                        .map_err(|e| compute_err("zip builder", e))?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder
                        .append_series(&combined)
                        .map_err(|e| compute_err("zip builder", e))?;
                }
            }
            _ => builder.append_null(),
        }
    }
    let out = builder.finish().into_series();
    Ok(Some(Column::new(name, out)))
}

/// Merge two maps into List(Struct{key, value1, value2}) for map_zip_with. Union of keys.
pub fn apply_map_zip_to_struct(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::StructChunked;
    use polars::chunked_array::builder::get_list_builder;
    use polars::datatypes::Field;
    use std::collections::BTreeMap;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "map_zip_to_struct needs two columns (map1, map2)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| compute_err("map_zip_with map1", e))?;
    let b_ca = b_series
        .list()
        .map_err(|e| compute_err("map_zip_with map2", e))?;
    let struct_dtype_in = a_ca.inner_dtype().clone();
    let (key_dtype, value_dtype) = match &struct_dtype_in {
        DataType::Struct(fields) => {
            let k = fields
                .iter()
                .find(|f| f.name == "key")
                .map(|f| f.dtype.clone())
                .unwrap_or(DataType::String);
            let v = fields
                .iter()
                .find(|f| f.name == "value")
                .map(|f| f.dtype.clone())
                .unwrap_or(DataType::String);
            (k, v)
        }
        _ => (DataType::String, DataType::String),
    };
    let out_struct_dtype = DataType::Struct(vec![
        Field::new("key".into(), key_dtype.clone()),
        Field::new("value1".into(), value_dtype.clone()),
        Field::new("value2".into(), value_dtype.clone()),
    ]);
    let mut builder = get_list_builder(&out_struct_dtype, 64, a_ca.len(), name.as_str().into());
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        match (opt_a, opt_b) {
            (Some(a_amort), Some(b_amort)) => {
                let a_list = a_amort.as_ref().as_list();
                let b_list = b_amort.as_ref().as_list();
                let mut key_to_vals: BTreeMap<String, (Series, Option<Series>, Option<Series>)> =
                    BTreeMap::new();
                for elem in a_list.amortized_iter().flatten() {
                    let s = elem.deep_clone();
                    if let Ok(st) = s.struct_() {
                        if let (Ok(k), Ok(v)) = (st.field_by_name("key"), st.field_by_name("value"))
                        {
                            let key_str: String = std::string::ToString::to_string(&k);
                            key_to_vals
                                .entry(key_str.clone())
                                .or_insert_with(|| (k.clone(), None, None))
                                .1 = Some(v);
                        }
                    }
                }
                for elem in b_list.amortized_iter().flatten() {
                    let s = elem.deep_clone();
                    if let Ok(st) = s.struct_() {
                        if let (Ok(k), Ok(v)) = (st.field_by_name("key"), st.field_by_name("value"))
                        {
                            let key_str: String = std::string::ToString::to_string(&k);
                            key_to_vals
                                .entry(key_str.clone())
                                .or_insert_with(|| (k.clone(), None, None))
                                .2 = Some(v);
                        }
                    }
                }
                let mut row_structs: Vec<Series> = Vec::new();
                for (_, (k_series, v1_opt, v2_opt)) in key_to_vals {
                    let mut k_renamed = k_series;
                    k_renamed.rename("key".into());
                    let v1_fallback = || {
                        Series::from_any_values_and_dtype(
                            PlSmallStr::EMPTY,
                            &[AnyValue::Null],
                            &value_dtype,
                            false,
                        )
                        .map_err(|e| compute_err("map_zip null fallback", e))
                    };
                    let mut v1_series = match v1_opt {
                        Some(s) => s,
                        None => v1_fallback()?,
                    };
                    v1_series.rename("value1".into());
                    let v2_fallback = || {
                        Series::from_any_values_and_dtype(
                            PlSmallStr::EMPTY,
                            &[AnyValue::Null],
                            &value_dtype,
                            false,
                        )
                        .map_err(|e| compute_err("map_zip null fallback", e))
                    };
                    let mut v2_series = match v2_opt {
                        Some(s) => s,
                        None => v2_fallback()?,
                    };
                    v2_series.rename("value2".into());
                    let len = k_renamed.len();
                    let fields: [&Series; 3] = [&k_renamed, &v1_series, &v2_series];
                    let st =
                        StructChunked::from_series(PlSmallStr::EMPTY, len, fields.iter().copied())
                            .map_err(|e| compute_err("map_zip struct", e))?
                            .into_series();
                    row_structs.push(st);
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &out_struct_dtype))
                        .map_err(|e| compute_err("map_zip builder", e))?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder
                        .append_series(&combined)
                        .map_err(|e| compute_err("map_zip builder", e))?;
                }
            }
            _ => builder.append_null(),
        }
    }
    let out = builder.finish().into_series();
    Ok(Some(Column::new(name, out)))
}

/// typeof: return dtype as string (PySpark typeof).
pub fn apply_typeof(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let dtype_str = format!("{:?}", series.dtype());
    let len = series.len();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        (0..len).map(|_| Some(dtype_str.clone())),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Helper: get two series as Int64Chunked with shared error context.
pub(super) fn binary_series_i64(
    a: &Series,
    b: &Series,
    ctx: &str,
) -> PolarsResult<(Int64Chunked, Int64Chunked)> {
    let ca_a = a.i64().map_err(|e| compute_err(ctx, e))?.clone();
    let ca_b = b.i64().map_err(|e| compute_err(ctx, e))?.clone();
    Ok((ca_a, ca_b))
}

/// Helper: get two series as Int32Chunked with shared error context.
pub(super) fn binary_series_i32(
    a: &Series,
    b: &Series,
    ctx: &str,
) -> PolarsResult<(Int32Chunked, Int32Chunked)> {
    let ca_a = a.i32().map_err(|e| compute_err(ctx, e))?.clone();
    let ca_b = b.i32().map_err(|e| compute_err(ctx, e))?.clone();
    Ok((ca_a, ca_b))
}

pub(super) fn try_narrow_integral_op(
    a: &Series,
    b: &Series,
    name: &str,
    op_name: &str,
    f: impl Fn(i64, i64) -> Option<i64>,
) -> PolarsResult<Option<Series>> {
    let result_type = match (a.dtype(), b.dtype()) {
        (DataType::Int8, DataType::Int8) => DataType::Int8,
        (DataType::Int16, DataType::Int16) => DataType::Int16,
        _ => return Ok(None),
    };
    let (a, b) = (a.cast(&DataType::Int32)?, b.cast(&DataType::Int32)?);
    let (ca_a, ca_b) = binary_series_i32(&a, &b, op_name)?;
    let values = ca_a.into_iter().zip(&ca_b).map(|(left, right)| {
        left.zip(right).and_then(|(left, right)| {
            f(left as i64, right as i64).and_then(|value| match &result_type {
                DataType::Int8 => i8::try_from(value).ok().map(i32::from),
                DataType::Int16 => i16::try_from(value).ok().map(i32::from),
                _ => unreachable!("matched a narrow integral result type"),
            })
        })
    });
    Ok(Some(
        Int32Chunked::from_iter_options(name.into(), values)
            .into_series()
            .cast(&result_type)?,
    ))
}

/// Helper: get two series as Float64Chunked with shared error context.
pub(super) fn binary_series_f64(
    a: &Series,
    b: &Series,
    ctx: &str,
) -> PolarsResult<(Float64Chunked, Float64Chunked)> {
    let a_f = a.cast(&DataType::Float64)?;
    let b_f = b.cast(&DataType::Float64)?;
    let ca_a = a_f.f64().map_err(|e| compute_err(ctx, e))?.clone();
    let ca_b = b_f.f64().map_err(|e| compute_err(ctx, e))?.clone();
    Ok((ca_a, ca_b))
}

pub(super) fn broadcast_binary_inputs(
    a: Series,
    b: Series,
    ctx: &str,
) -> PolarsResult<(Series, Series)> {
    // A scalar broadcasts to an empty input as an empty result. Treating the scalar
    // as a one-row frame would incorrectly turn an empty DataFrame into one row.
    if (a.is_empty() && b.len() <= 1) || (b.is_empty() && a.len() <= 1) {
        return Ok((a.slice(0, 0), b.slice(0, 0)));
    }
    let len = a.len().max(b.len());
    if (a.len() != 1 && a.len() != len) || (b.len() != 1 && b.len() != len) {
        return Err(PolarsError::ShapeMismatch(
            format!("{ctx}: inputs must have equal lengths or be scalars").into(),
        ));
    }
    let a = if a.len() == 1 && len > 1 {
        a.new_from_index(0, len)
    } else {
        a
    };
    let b = if b.len() == 1 && len > 1 {
        b.new_from_index(0, len)
    } else {
        b
    };
    Ok((a, b))
}

/// Helper: convert any numeric-or-string series to Float64Chunked with PySpark-like semantics.
///
/// - Numeric types are cast to Float64.
/// - Utf8/string values are trimmed and parsed as f64; invalid strings become null.
/// - Null values remain null.
pub(super) fn series_to_f64_pyspark(s: &Series, ctx: &str) -> PolarsResult<Float64Chunked> {
    match s.dtype() {
        DataType::String => {
            let name = s.name();
            let ca = s.str().map_err(|e| compute_err(ctx, e))?;
            let out = Float64Chunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_s| {
                    opt_s.and_then(|raw| {
                        let trimmed = raw.trim();
                        if trimmed.is_empty() {
                            return None;
                        }
                        trimmed.parse::<f64>().ok()
                    })
                }),
            );
            Ok(out)
        }
        DataType::Null => {
            // All-null column – return a null Float64Chunked of same length.
            Ok(Float64Chunked::full_null(PlSmallStr::EMPTY, s.len()))
        }
        DataType::Decimal(..) => {
            // Polars' direct Decimal-to-Float64 cast can round large values up.
            // Parsing the exact decimal text gives the correctly rounded f64.
            let text = s.cast(&DataType::String).map_err(|e| compute_err(ctx, e))?;
            series_to_f64_pyspark(&text, ctx)
        }
        // For all other dtypes, rely on Polars cast to Float64.
        _ => {
            let casted = s
                .cast(&DataType::Float64)
                .map_err(|e| compute_err(ctx, e))?;
            casted.f64().cloned().map_err(|e| compute_err(ctx, e))
        }
    }
}

/// Binary arithmetic helper used for Python Column operators with PySpark-style string coercion.
///
/// Always returns a Float64 series; string inputs are coerced to numbers when possible,
/// invalid strings become null. Numeric-only inputs are upcast to Float64.
/// Broadcasts scalar (length-1) operands to match the other series length (PySpark parity).
#[allow(clippy::useless_conversion)]
pub(super) fn pyspark_binary_arith(
    columns: &mut [Column],
    ctx: &str,
    op: fn(f64, f64) -> f64,
) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            format!("{ctx} needs two columns").into(),
        ));
    }

    let name = columns[0].field().into_owned().name;
    let a_s = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_s = std::mem::take(&mut columns[1]).take_materialized_series();

    let mut ca_a = series_to_f64_pyspark(&a_s, ctx)?;
    let mut ca_b = series_to_f64_pyspark(&b_s, ctx)?;

    // Broadcast scalar (length-1) to match the other series (#179)
    let len_a = ca_a.len();
    let len_b = ca_b.len();
    if len_a == 1 && len_b > 1 {
        let val = ca_a.get(0);
        ca_a = Float64Chunked::from_iter_options(
            name.as_str().into(),
            std::iter::repeat_n(val, len_b),
        );
    } else if len_b == 1 && len_a > 1 {
        let val = ca_b.get(0);
        ca_b = Float64Chunked::from_iter_options(
            name.as_str().into(),
            std::iter::repeat_n(val, len_a),
        );
    }

    let out = Float64Chunked::from_iter_options(
        name.as_str().into(),
        ca_a.into_iter()
            .zip(ca_b.into_iter())
            .map(|(oa, ob)| match (oa, ob) {
                (Some(a), Some(b)) => Some(op(a, b)),
                _ => None,
            }),
    )
    .into_series();

    Ok(Some(Column::new(name, out)))
}

/// Keep decimal and integral Column arithmetic in exact decimal coefficients.
/// Returns `None` for inputs that should use the regular Float64 coercion path.
pub(super) fn pyspark_decimal_binary_arith(
    columns: &mut [Column],
    ctx: &str,
    op: crate::decimal_arithmetic::BinaryOp,
    ansi_enabled: bool,
    allow_precision_loss: bool,
) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            format!("{ctx} needs two columns").into(),
        ));
    }
    let left_type = columns[0].dtype().clone();
    let right_type = columns[1].dtype().clone();
    let Some(output_type) =
        crate::decimal_arithmetic::result_type(&left_type, &right_type, op, allow_precision_loss)
    else {
        return Ok(None);
    };

    let name = columns[0].field().into_owned().name;
    let left = std::mem::take(&mut columns[0]).take_materialized_series();
    let right = std::mem::take(&mut columns[1]).take_materialized_series();
    let output =
        crate::decimal_arithmetic::evaluate(&left, &right, op, &output_type, ansi_enabled)?;
    Ok(Some(Column::new(name, output)))
}
