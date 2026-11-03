use super::*;
use crate::column::Column;
use crate::functions::{col, lit_bool, lit_f64, lit_i32, lit_i64, lit_str};
use polars::prelude::{DataType, IntoColumn, IntoLazy, NamedFrom, Series, df};

#[test]
fn test_col_creates_column() {
    let column = col("test");
    assert_eq!(column.name(), "test");
}

#[test]
fn test_lit_i32() {
    let column = lit_i32(42);
    // The column should have a default name since it's a literal
    assert_eq!(column.name(), "<expr>");
}

#[test]
fn test_lit_i64() {
    let column = lit_i64(123456789012345i64);
    assert_eq!(column.name(), "<expr>");
}

#[test]
fn count_typed_literal_counts_every_row() {
    let input = df!("value" => [Some(1i64), None, Some(3), None]).unwrap();
    let out = input
        .lazy()
        .select([count(&lit_i64(1)).into_expr().alias("count_all")])
        .collect()
        .unwrap();
    assert_eq!(
        out.column("count_all").unwrap().i64().unwrap().get(0),
        Some(4)
    );
}

#[test]
fn test_lit_f64() {
    let column = lit_f64(std::f64::consts::PI);
    assert_eq!(column.name(), "<expr>");
}

#[test]
fn test_lit_bool() {
    let column = lit_bool(true);
    assert_eq!(column.name(), "<expr>");
}

#[test]
fn test_lit_str() {
    let column = lit_str("hello");
    assert_eq!(column.name(), "<expr>");
}

#[test]
fn cast_null_literal_to_int_and_long() {
    // Simulate F.lit(None).cast(IntegerType/LongType): casting from Null to Int32/Int64
    // should yield typed null columns instead of erroring (#1028).
    let s = Series::new_null("x".into(), 3);

    // Cast via the lower-level helper to be explicit about the target type.
    let col_i32 =
        crate::udfs::apply_string_to_int(s.clone().into_column(), true, DataType::Int32, false)
            .expect("cast to Int32 should succeed")
            .expect("column should be present");
    let col_i64 = crate::udfs::apply_string_to_int(s.into_column(), true, DataType::Int64, false)
        .expect("cast to Int64 should succeed")
        .expect("column should be present");

    assert_eq!(col_i32.dtype(), &DataType::Int32);
    assert_eq!(col_i64.dtype(), &DataType::Int64);
    assert!(col_i32.is_null().all());
    assert!(col_i64.is_null().all());
}

#[test]
fn cast_string_and_float_columns_to_smallint() {
    let strings = Series::new("x".into(), vec![Some("12"), Some("40000"), None]);
    let cast_strings =
        crate::udfs::apply_string_to_int(strings.into_column(), false, DataType::Int16, false)
            .unwrap()
            .unwrap();
    assert_eq!(cast_strings.dtype(), &DataType::Int16);
    assert_eq!(
        cast_strings.i16().unwrap().into_iter().collect::<Vec<_>>(),
        vec![Some(12), None, None]
    );

    let floats = Series::new("x".into(), vec![Some(12.9f64), Some(40000.0), None]);
    let cast_floats = crate::udfs::apply_string_to_int(
        floats.clone().into_column(),
        false,
        DataType::Int16,
        false,
    )
    .unwrap()
    .unwrap();
    assert_eq!(cast_floats.dtype(), &DataType::Int16);
    assert_eq!(
        cast_floats.i16().unwrap().into_iter().collect::<Vec<_>>(),
        vec![Some(12), None, None]
    );
    assert!(
        crate::udfs::apply_string_to_int(floats.into_column(), true, DataType::Int16, false,)
            .is_err()
    );
}

#[test]
fn cast_smallint_column_to_integer_types() {
    let input = Series::new("x".into(), vec![Some(12_i16), None, Some(-7)]);

    for target in [DataType::Int16, DataType::Int32, DataType::Int64] {
        let cast = crate::udfs::apply_string_to_int(
            input.clone().into_column(),
            true,
            target.clone(),
            false,
        )
        .unwrap()
        .unwrap();

        assert_eq!(cast.dtype(), &target);
        match target {
            DataType::Int16 => assert_eq!(
                cast.i16().unwrap().into_iter().collect::<Vec<_>>(),
                vec![Some(12), None, Some(-7)]
            ),
            DataType::Int32 => assert_eq!(
                cast.i32().unwrap().into_iter().collect::<Vec<_>>(),
                vec![Some(12), None, Some(-7)]
            ),
            DataType::Int64 => assert_eq!(
                cast.i64().unwrap().into_iter().collect::<Vec<_>>(),
                vec![Some(12), None, Some(-7)]
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_create_map_empty() {
    // PySpark F.create_map() with no args: column of empty maps (#275).
    let empty_col = create_map(&[]).unwrap();
    let df = df!("id" => &[1i64, 2i64]).unwrap();
    let out = df
        .lazy()
        .with_columns([empty_col.into_expr().alias("m")])
        .collect()
        .unwrap();
    assert_eq!(out.height(), 2);
    let m = out.column("m").unwrap();
    assert_eq!(m.len(), 2);
    let list = m.list().unwrap();
    for i in 0..2 {
        let row = list.get(i).unwrap();
        assert_eq!(row.len(), 0);
    }
}

#[test]
fn test_count_aggregation() {
    let column = col("value");
    let result = count(&column);
    assert_eq!(result.name(), "count(value)");
}

#[test]
fn test_sum_aggregation() {
    let column = col("value");
    let result = sum(&column);
    assert_eq!(result.name(), "sum(value)");
}

#[test]
fn test_avg_aggregation() {
    let column = col("value");
    let result = avg(&column);
    assert_eq!(result.name(), "avg(value)");
}

#[test]
fn test_max_aggregation() {
    let column = col("value");
    let result = max(&column);
    assert_eq!(result.name(), "max(value)");
}

#[test]
fn test_min_aggregation() {
    let column = col("value");
    let result = min(&column);
    assert_eq!(result.name(), "min(value)");
}

#[test]
fn test_when_then_otherwise() {
    // Create a simple DataFrame
    let df = df!(
        "age" => &[15, 25, 35]
    )
    .unwrap();

    // Build a when-then-otherwise expression
    let age_col = col("age");
    let condition = age_col.gt(polars::prelude::lit(18));
    let result = when(&condition)
        .then(&lit_str("adult"))
        .otherwise(&lit_str("minor"));

    // Apply the expression
    let result_df = df
        .lazy()
        .with_column(result.into_expr().alias("status"))
        .collect()
        .unwrap();

    // Verify the result
    let status_col = result_df.column("status").unwrap();
    let values: Vec<Option<&str>> = status_col.str().unwrap().into_iter().collect();

    assert_eq!(values[0], Some("minor")); // age 15 < 18
    assert_eq!(values[1], Some("adult")); // age 25 > 18
    assert_eq!(values[2], Some("adult")); // age 35 > 18
}

/// #680: when/then/otherwise with numeric then/else (condition Boolean, then/else any type).
#[test]
fn test_when_then_otherwise_numeric_then_else() {
    let df = df!("a" => &[1i64, 3, 5]).unwrap();
    let condition = col("a").gt(polars::prelude::lit(2));
    let result = when(&condition).then(&lit_i64(100)).otherwise(&lit_i64(0));
    let result_df = df
        .lazy()
        .with_column(result.into_expr().alias("out"))
        .collect()
        .unwrap();
    let out = result_df.column("out").unwrap();
    // Polars may infer i32 for when/then/otherwise with small literals; cast to i64 for stable assertion.
    let out_i64 = out.cast(&DataType::Int64).unwrap();
    let values: Vec<Option<i64>> = out_i64.i64().unwrap().into_iter().collect();
    assert_eq!(values, vec![Some(0), Some(100), Some(100)]);
}

#[test]
fn test_coalesce_returns_first_non_null() {
    // Create a DataFrame with some nulls
    let df = df!(
        "a" => &[Some(1), None, None],
        "b" => &[None, Some(2), None],
        "c" => &[None, None, Some(3)]
    )
    .unwrap();

    let col_a = col("a");
    let col_b = col("b");
    let col_c = col("c");
    let result = coalesce(&[&col_a, &col_b, &col_c]).unwrap();

    // Apply the expression
    let result_df = df
        .lazy()
        .with_column(result.into_expr().alias("coalesced"))
        .collect()
        .unwrap();

    // Verify the result
    let coalesced_col = result_df.column("coalesced").unwrap();
    let values: Vec<Option<i32>> = coalesced_col.i32().unwrap().into_iter().collect();

    assert_eq!(values[0], Some(1)); // First non-null is 'a'
    assert_eq!(values[1], Some(2)); // First non-null is 'b'
    assert_eq!(values[2], Some(3)); // First non-null is 'c'
}

#[test]
fn test_coalesce_with_literal_fallback() {
    // Create a DataFrame with all nulls in one row
    let df = df!(
        "a" => &[Some(1), None],
        "b" => &[None::<i32>, None::<i32>]
    )
    .unwrap();

    let col_a = col("a");
    let col_b = col("b");
    let fallback = lit_i32(0);
    let result = coalesce(&[&col_a, &col_b, &fallback]).unwrap();

    // Apply the expression
    let result_df = df
        .lazy()
        .with_column(result.into_expr().alias("coalesced"))
        .collect()
        .unwrap();

    // Verify the result
    let coalesced_col = result_df.column("coalesced").unwrap();
    let values: Vec<Option<i32>> = coalesced_col.i32().unwrap().into_iter().collect();

    assert_eq!(values[0], Some(1)); // First non-null is 'a'
    assert_eq!(values[1], Some(0)); // All nulls, use fallback
}

#[test]
fn test_coalesce_empty_returns_error() {
    let columns: [&Column; 0] = [];
    let err = coalesce(&columns).unwrap_err();
    assert!(
        err.to_string()
            .contains("coalesce requires at least one column")
    );
}

#[test]
fn test_cast_double_string_column_strict_ok() {
    // All values parse as doubles, so strict cast should succeed.
    let df = df!(
        "s" => &["123", " 45.5 ", "0"]
    )
    .unwrap();

    let s_col = col("s");
    let cast_col = cast(&s_col, "double").unwrap();

    let out = df
        .lazy()
        .with_column(cast_col.into_expr().alias("v"))
        .collect()
        .unwrap();

    let v = out.column("v").unwrap();
    let vals: Vec<Option<f64>> = v.f64().unwrap().into_iter().collect();
    assert_eq!(vals, vec![Some(123.0), Some(45.5), Some(0.0)]);
}

#[test]
fn test_try_cast_double_string_column_invalid_to_null() {
    // Invalid numeric strings should become null under try_cast / try_to_number.
    let df = df!(
        "s" => &["123", " 45.5 ", "abc", ""]
    )
    .unwrap();

    let s_col = col("s");
    let try_cast_col = try_cast(&s_col, "double").unwrap();

    let out = df
        .lazy()
        .with_column(try_cast_col.into_expr().alias("v"))
        .collect()
        .unwrap();

    let v = out.column("v").unwrap();
    let vals: Vec<Option<f64>> = v.f64().unwrap().into_iter().collect();
    assert_eq!(vals, vec![Some(123.0), Some(45.5), None, None]);
}

/// #649: cast/try_cast double to int: truncation; NaN/inf -> null (try_cast) or error (cast).
#[test]
fn test_cast_double_to_int_nan_to_null() {
    use polars::prelude::df;

    let df = df!(
        "x" => &[1.7, 2.3, f64::NAN, f64::INFINITY]
    )
    .unwrap();

    let col_x = col("x");
    let try_cast_col = try_cast(&col_x, "int").unwrap();

    let out = df
        .lazy()
        .with_column(try_cast_col.into_expr().alias("v"))
        .collect()
        .unwrap();

    let v = out.column("v").unwrap();
    let vals: Vec<Option<i32>> = v.i32().unwrap().into_iter().collect();
    assert_eq!(vals, vec![Some(1), Some(2), None, None]);
}

#[test]
fn test_to_number_and_try_to_number_numerics_and_strings() {
    // Mixed numeric types should be cast to double; invalid strings become null only for try_to_number.
    let df = df!(
        "i" => &[1i32, 2, 3],
        "f" => &[1.5f64, 2.5, 3.5],
        "s" => &["10", "20.5", "xyz"]
    )
    .unwrap();

    let i_col = col("i");
    let f_col = col("f");
    let s_col = col("s");

    let to_number_i = to_number(&i_col, None).unwrap();
    let to_number_f = to_number(&f_col, None).unwrap();
    let try_to_number_s = try_to_number(&s_col, None).unwrap();

    let out = df
        .lazy()
        .with_columns([
            to_number_i.into_expr().alias("i_num"),
            to_number_f.into_expr().alias("f_num"),
            try_to_number_s.into_expr().alias("s_num"),
        ])
        .collect()
        .unwrap();

    let i_num = out.column("i_num").unwrap();
    let f_num = out.column("f_num").unwrap();
    let s_num = out.column("s_num").unwrap();

    let i_vals: Vec<Option<f64>> = i_num.f64().unwrap().into_iter().collect();
    let f_vals: Vec<Option<f64>> = f_num.f64().unwrap().into_iter().collect();
    let s_vals: Vec<Option<f64>> = s_num.f64().unwrap().into_iter().collect();

    assert_eq!(i_vals, vec![Some(1.0), Some(2.0), Some(3.0)]);
    assert_eq!(f_vals, vec![Some(1.5), Some(2.5), Some(3.5)]);
    assert_eq!(s_vals, vec![Some(10.0), Some(20.5), None]);
}
