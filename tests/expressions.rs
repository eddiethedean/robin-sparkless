//! Expressions, casts, string/struct/map ops, coercion, is_in, case sensitivity.
//!
//! Merged from: issue_235, issue_544, issue_541, issue_542, issue_547, issue_549,
//! issue_556, issue_557, issue_637_638_650, issue_555, issue_649_635, issue_636, issue_548.

mod common;

use chrono::NaiveDate;
use common::spark;
use polars::prelude::{DataType, NamedFrom, Series};
use robin_sparkless::functions::{col, create_map, lit_i64, lit_str, named_struct, substring};
use robin_sparkless::plan;
use robin_sparkless::{DataFrame, cast, try_cast};
use serde_json::json;

// ---------- issue_235 (helpers: no common) ----------

fn df_with_string_column() -> DataFrame {
    let s = Series::new("str_col".into(), &["123", "456"]);
    let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
    DataFrame::from_polars(pl_df)
}

fn df_with_date_column() -> DataFrame {
    let d1 = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    let s = Series::new("dt".into(), [d1, d2])
        .cast(&DataType::Date)
        .unwrap();
    let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
    DataFrame::from_polars(pl_df)
}

#[test]
fn issue_235_string_eq_numeric_literal_in_filter() {
    let df = df_with_string_column();
    let expr = col("str_col").eq(lit_i64(123).into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(
        out.count().unwrap(),
        1,
        "filter(str_col == 123) should return one row"
    );
}

#[test]
fn issue_602_filter_string_column_eq_numeric_literal() {
    let df = df_with_string_column();
    let expr = col("str_col").eq(lit_i64(123).into_expr()).into_expr();
    let out = df
        .filter(expr)
        .expect("issue #602: filter(string_col == 123) must succeed with coercion");
    assert_eq!(out.count().unwrap(), 1);
}

#[test]
fn issue_235_literal_eq_string_column_symmetric_form() {
    let df = df_with_string_column();
    let expr = lit_i64(123).eq(col("str_col").into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(out.count().unwrap(), 1);
}

#[test]
fn issue_235_string_gt_numeric_literal_uses_numeric_semantics() {
    let df = df_with_string_column();
    let expr = col("str_col").gt(lit_i64(200).into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(out.count().unwrap(), 1);
}

#[test]
fn issue_235_string_eq_numeric_with_invalid_string_non_matching() {
    let s = Series::new("str_col".into(), &["abc", "123"]);
    let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
    let df = DataFrame::from_polars(pl_df);
    let expr = col("str_col").eq(lit_i64(123).into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(out.count().unwrap(), 1);
}

#[test]
fn issue_265_date_column_eq_string_literal() {
    let df = df_with_date_column();
    let expr = col("dt").eq(lit_str("2025-01-01").into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(out.count().unwrap(), 1);
}

#[test]
fn issue_265_date_column_ne_string_literal() {
    let df = df_with_date_column();
    let expr = col("dt").neq(lit_str("2025-01-01").into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(out.count().unwrap(), 1);
}

// ---------- issue_544 ----------

#[test]
fn issue_544_int_to_string_cast() {
    let spark = spark();
    let pl = polars::prelude::df!["x" => &[1i64, 2i64]].unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let out = df
        .select_exprs(vec![
            cast(&col("x"), "string").unwrap().alias("s").into_expr(),
        ])
        .unwrap();
    let rows = out.collect_as_json_rows().unwrap();
    assert_eq!(rows[0].get("s").and_then(|v| v.as_str()), Some("1"));
    assert_eq!(rows[1].get("s").and_then(|v| v.as_str()), Some("2"));
}

#[test]
fn issue_544_string_to_int_cast_and_try_cast() {
    let spark = spark();
    let pl = polars::prelude::df!["x" => &["1", "2", "not_int"]].unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let cast_res = cast(&col("x"), "bigint").unwrap();
    let out = df
        .select_exprs(vec![cast_res.alias("y").into_expr()])
        .and_then(|d| d.collect_as_json_rows());
    assert!(out.is_err());

    let try_col = try_cast(&col("x"), "bigint").unwrap();
    let out_ok = df
        .select_exprs(vec![try_col.alias("y").into_expr()])
        .unwrap();
    let rows = out_ok.collect_as_json_rows().unwrap();
    assert_eq!(rows[0].get("y").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(rows[1].get("y").and_then(|v| v.as_i64()), Some(2));
    assert!(rows[2].get("y").unwrap().is_null());
}

#[test]
fn issue_544_string_to_boolean_cast_and_try_cast() {
    let spark = spark();
    let pl = polars::prelude::df!["x" => &["true", "false", "1", "0", "invalid"]].unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let cast_bool = cast(&col("x"), "boolean").unwrap();
    let out = df
        .select_exprs(vec![cast_bool.alias("b").into_expr()])
        .and_then(|d| d.collect_as_json_rows());
    assert!(out.is_err());

    let try_bool = try_cast(&col("x"), "boolean").unwrap();
    let out_ok = df
        .select_exprs(vec![try_bool.alias("b").into_expr()])
        .unwrap();
    let rows = out_ok.collect_as_json_rows().unwrap();
    assert_eq!(rows[0].get("b").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(rows[1].get("b").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(rows[2].get("b").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(rows[3].get("b").and_then(|v| v.as_bool()), Some(false));
    assert!(rows[4].get("b").unwrap().is_null());
}

// ---------- issue_541 ----------

#[test]
fn issue_541_rust_api_with_field() {
    let spark = spark();
    let pl = polars::prelude::df![
        "id" => &["1"],
        "value_1" => &[10i64],
        "value_2" => &["x"],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);
    let struct_df = df
        .select_exprs(vec![
            col("id").clone().into_expr(),
            named_struct(&[("value_1", &col("value_1")), ("value_2", &col("value_2"))])
                .alias("my_struct")
                .into_expr(),
        ])
        .unwrap();
    let updated = struct_df
        .with_column(
            "my_struct",
            &col("my_struct").with_field("value_1", &lit_i64(99)),
        )
        .unwrap();
    let extracted = updated
        .with_column("value_1_out", &col("my_struct").get_field("value_1"))
        .unwrap();
    let rows = extracted.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("value_1_out").and_then(|v| v.as_i64()),
        Some(99)
    );
}

#[test]
fn issue_541_plan_with_field() {
    let session = spark();
    let data = vec![vec![json!("1"), json!({"value_1": 10, "value_2": "x"})]];
    let schema = vec![
        ("id".to_string(), "string".to_string()),
        (
            "my_struct".to_string(),
            "struct<value_1:bigint,value_2:string>".to_string(),
        ),
    ];
    let plan = vec![
        json!({
            "op": "withColumn",
            "payload": {
                "name": "my_struct",
                "expr": {
                    "fn": "withField",
                    "args": [
                        {"col": "my_struct"},
                        {"lit": "value_1"},
                        {"lit": 99}
                    ]
                }
            }
        }),
        json!({
            "op": "withColumn",
            "payload": {
                "name": "value_1_out",
                "expr": {"fn": "get_field", "args": [{"col": "my_struct"}, {"lit": "value_1"}]}
            }
        }),
    ];
    let df = plan::execute_plan(&session, data, schema, &plan).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("value_1_out").and_then(|v| v.as_i64()),
        Some(99)
    );
}

// ---------- issue_542 ----------

#[test]
fn issue_542_rust_api_create_map() {
    let spark = spark();
    let pl = polars::prelude::df![
        "val1" => &["a"],
        "val2" => &[1i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);
    let map_col = create_map(&[
        &lit_str("key1"),
        &col("val1"),
        &lit_str("key2"),
        &col("val2"),
    ])
    .unwrap();
    let out = df
        .with_column("map_col", &map_col)
        .unwrap()
        .select(vec!["map_col"])
        .unwrap();
    let rows = out.count().unwrap();
    assert_eq!(rows, 1);
    let with_keys = out
        .with_column("keys", &robin_sparkless::map_keys(&col("map_col")))
        .unwrap();
    let keys_row = with_keys.collect_as_json_rows().unwrap();
    assert_eq!(keys_row.len(), 1);
}

#[test]
fn issue_542_plan_create_map_op() {
    let session = spark();
    let data = vec![vec![json!("a"), json!(1)]];
    let schema = vec![
        ("val1".to_string(), "string".to_string()),
        ("val2".to_string(), "bigint".to_string()),
    ];
    let plan = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "map_col",
            "expr": {
                "op": "createMap",
                "args": [
                    {"lit": "key1"},
                    {"col": "val1"},
                    {"lit": "key2"},
                    {"col": "val2"}
                ]
            }
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan).unwrap();
    assert_eq!(df.count().unwrap(), 1);
}

#[test]
fn issue_542_plan_create_map_fn() {
    let session = spark();
    let data = vec![vec![json!("a"), json!(1)]];
    let schema = vec![
        ("val1".to_string(), "string".to_string()),
        ("val2".to_string(), "bigint".to_string()),
    ];
    let plan = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "map_col",
            "expr": {
                "fn": "create_map",
                "args": [
                    {"lit": "key1"},
                    {"col": "val1"},
                    {"lit": "key2"},
                    {"col": "val2"}
                ]
            }
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan).unwrap();
    assert_eq!(df.count().unwrap(), 1);
}

#[test]
fn issue_578_create_map_empty_returns_empty_object_not_null() {
    let spark = spark();
    let df = spark
        .create_dataframe_from_rows(
            vec![vec![json!(1)]],
            vec![("id".to_string(), "bigint".to_string())],
        )
        .unwrap();
    let empty_map = create_map(&[]).unwrap();
    let out = df.with_column("m", &empty_map).unwrap();
    let rows = out.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    let m = rows[0].get("m").expect("column m");
    assert!(!m.is_null());
    let obj = m.as_object().expect("m must be JSON object (empty map)");
    assert!(obj.is_empty());
}

// ---------- issue_547 ----------

#[test]
fn issue_547_plan_op_regexp_extract_all() {
    let session = spark();
    let data = vec![vec![json!("hello world")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "m",
            "expr": {
                "op": "regexp_extract_all",
                "args": [{"col": "s"}, {"lit": "\\w+"}]
            }
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains_key("m"));
}

#[test]
fn issue_547_plan_op_levenshtein() {
    let session = spark();
    let data = vec![vec![json!("kitten"), json!("sitting")]];
    let schema = vec![
        ("a".to_string(), "string".to_string()),
        ("b".to_string(), "string".to_string()),
    ];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "dist",
            "expr": {
                "op": "levenshtein",
                "args": [{"col": "a"}, {"col": "b"}]
            }
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("dist").and_then(|v| v.as_i64()), Some(3));
}

#[test]
fn issue_547_plan_op_translate() {
    let session = spark();
    let data = vec![vec![json!("hello")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "t",
            "expr": {
                "op": "translate",
                "args": [{"col": "s"}, {"lit": "el"}, {"lit": "ip"}]
            }
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("t").and_then(|v| v.as_str()), Some("hippo"));
}

// ---------- issue_549 ----------

#[test]
fn issue_549_plan_op_format_string() {
    let session = spark();
    let data = vec![vec![json!("Alice"), json!(123)]];
    let schema = vec![
        ("Name".to_string(), "string".to_string()),
        ("IntegerValue".to_string(), "bigint".to_string()),
    ];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "formatted",
            "expr": {
                "op": "format_string",
                "args": [{"lit": "%s: %d"}, {"col": "Name"}, {"col": "IntegerValue"}]
            }
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("formatted").and_then(|v| v.as_str()),
        Some("Alice: 123")
    );
}

#[test]
fn issue_549_plan_op_log_one_arg() {
    let session = spark();
    let data = vec![vec![json!(1.0)]];
    let schema = vec![("x".to_string(), "double".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "ln_x",
            "expr": {"op": "log", "args": [{"col": "x"}]}
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    let v = rows[0].get("ln_x").and_then(|v| v.as_f64()).unwrap();
    assert!((v - 0.0).abs() < 1e-10, "ln(1) = 0, got {}", v);
}

// ---------- issue_556 ----------

#[test]
fn issue_556_plan_lit_minus_col() {
    let session = spark();
    let data = vec![vec![json!(10)], vec![json!(20)]];
    let schema = vec![("x".to_string(), "bigint".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "rev",
            "expr": {"op": "sub", "left": {"lit": 1}, "right": {"col": "x"}}
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("rev").and_then(|v| v.as_i64()), Some(-9));
    assert_eq!(rows[1].get("rev").and_then(|v| v.as_i64()), Some(-19));
}

#[test]
fn issue_556_plan_lit_times_col() {
    let session = spark();
    let data = vec![vec![json!(3)], vec![json!(5)]];
    let schema = vec![("x".to_string(), "bigint".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "scaled",
            "expr": {"op": "mul", "left": {"lit": 100}, "right": {"col": "x"}}
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("scaled").and_then(|v| v.as_i64()), Some(300));
    assert_eq!(rows[1].get("scaled").and_then(|v| v.as_i64()), Some(500));
}

// ---------- issue_557 ----------

#[test]
fn issue_557_substring_1based_positive_start() {
    let session = spark();
    let data = vec![vec![json!("Hello")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let df = session
        .create_dataframe_from_rows(data, schema)
        .unwrap()
        .select_exprs(vec![
            substring(&col("s"), 2, Some(3)).alias("out").into_expr(),
        ])
        .unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("out").and_then(|v| v.as_str()), Some("ell"));
}

#[test]
fn issue_557_substring_negative_start_from_end() {
    let session = spark();
    let data = vec![vec![json!("Spark SQL")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let df = session
        .create_dataframe_from_rows(data, schema)
        .unwrap()
        .select_exprs(vec![
            substring(&col("s"), -3, None).alias("out").into_expr(),
        ])
        .unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("out").and_then(|v| v.as_str()), Some("SQL"));
}

#[test]
fn issue_557_substring_zero_length_empty_string() {
    let session = spark();
    let data = vec![vec![json!("Hello")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let df = session
        .create_dataframe_from_rows(data, schema)
        .unwrap()
        .select_exprs(vec![
            substring(&col("s"), 1, Some(0)).alias("out").into_expr(),
        ])
        .unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("out").and_then(|v| v.as_str()), Some(""));
}

#[test]
fn issue_557_substring_negative_length_empty_string() {
    let session = spark();
    let data = vec![vec![json!("Hello")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let df = session
        .create_dataframe_from_rows(data, schema)
        .unwrap()
        .select_exprs(vec![
            substring(&col("s"), 1, Some(-1)).alias("out").into_expr(),
        ])
        .unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("out").and_then(|v| v.as_str()), Some(""));
}

// ---------- issue_637_638_650 ----------

#[test]
fn plan_filter_isin_op_int64() {
    let spark = spark();
    let schema = vec![("value".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]];
    let plan = vec![json!({
        "op": "filter",
        "payload": {"op": "isin", "left": {"col": "value"}, "right": {"lit": [1, 3]}}
    })];
    let result = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let rows_out = result.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows_out.len(), 2);
    let values: std::collections::HashSet<i64> = rows_out
        .iter()
        .map(|r| r.get("value").and_then(|v| v.as_i64()).unwrap())
        .collect();
    assert_eq!(values, [1, 3].into_iter().collect());
}

#[test]
fn plan_filter_isin_fn_int64() {
    let spark = spark();
    let schema = vec![("id".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(10)], vec![json!(20)], vec![json!(30)]];
    let plan = vec![json!({
        "op": "filter",
        "payload": {"fn": "isin", "args": [{"col": "id"}, {"lit": 10}, {"lit": 30}]}
    })];
    let result = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let rows_out = result.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows_out.len(), 2);
    let ids: std::collections::HashSet<i64> = rows_out
        .iter()
        .map(|r| r.get("id").and_then(|v| v.as_i64()).unwrap())
        .collect();
    assert_eq!(ids, [10, 30].into_iter().collect());
}

#[test]
fn plan_filter_isin_empty_yields_zero_rows() {
    let spark = spark();
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let rows = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let plan = vec![json!({
        "op": "filter",
        "payload": {"op": "isin", "left": {"col": "id"}, "right": {"lit": []}}
    })];
    let result = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let rows_out = result.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows_out.len(), 0);
}

#[test]
fn plan_filter_isin_op_string() {
    let spark = spark();
    let schema = vec![("name".to_string(), "string".to_string())];
    let rows = vec![
        vec![json!("alice")],
        vec![json!("bob")],
        vec![json!("carol")],
    ];
    let plan = vec![json!({
        "op": "filter",
        "payload": {"op": "isin", "left": {"col": "name"}, "right": {"lit": ["alice", "carol"]}}
    })];
    let result = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let rows_out = result.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows_out.len(), 2);
    let names: std::collections::HashSet<String> = rows_out
        .iter()
        .filter_map(|r| r.get("name").and_then(|v| v.as_str()).map(String::from))
        .collect();
    assert_eq!(
        names,
        ["alice".into(), "carol".into()].into_iter().collect()
    );
}

// ---------- issue_555 ----------

#[test]
fn issue_555_select_col_lowercase_resolves_to_mixed_case_schema() {
    let session = spark();
    let data = vec![vec![json!("Alice"), json!(25)]];
    let schema = vec![
        ("Name".to_string(), "string".to_string()),
        ("Age".to_string(), "bigint".to_string()),
    ];
    let plan_ops = vec![json!({
        "op": "select",
        "payload": {
            "columns": [{"name": "age_out", "expr": {"col": "age"}}]
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("age_out").and_then(|v| v.as_i64()),
        Some(25),
        "col(\"age\") must resolve to column \"Age\""
    );
}

// ---------- issue_649_635 ----------

#[test]
fn plan_select_cast_int_to_string() {
    let spark = spark();
    let schema = vec![("n".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(10)], vec![json!(20)]];
    let plan_steps = vec![json!({
        "op": "select",
        "payload": [{"name": "n", "expr": {"fn": "cast", "args": [{"col": "n"}, {"lit": "string"}]}}]
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("n").and_then(|v| v.as_str()), Some("10"));
    assert_eq!(out[1].get("n").and_then(|v| v.as_str()), Some("20"));
}

#[test]
fn plan_with_column_try_cast_string_to_bigint() {
    let spark = spark();
    let schema = vec![("x".to_string(), "string".to_string())];
    let rows = vec![
        vec![json!("1")],
        vec![json!("2")],
        vec![json!("not_a_number")],
    ];
    let plan_steps = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "y",
            "expr": {"fn": "try_cast", "args": [{"col": "x"}, {"lit": "bigint"}]}
        }
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].get("y").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(out[1].get("y").and_then(|v| v.as_i64()), Some(2));
    assert!(out[2].get("y").map(|v| v.is_null()).unwrap_or(false));
}

#[test]
fn plan_select_cast_with_alias() {
    let spark = spark();
    let schema = vec![
        ("a".to_string(), "bigint".to_string()),
        ("b".to_string(), "bigint".to_string()),
    ];
    let rows = vec![vec![json!(1), json!(2)], vec![json!(3), json!(4)]];
    let plan_steps = vec![json!({
        "op": "select",
        "payload": [
            {"name": "a_str", "expr": {"fn": "cast", "args": [{"col": "a"}, {"lit": "string"}]}},
            {"name": "b", "expr": {"col": "b"}}
        ]
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("a_str").and_then(|v| v.as_str()), Some("1"));
    assert_eq!(out[0].get("b").and_then(|v| v.as_i64()), Some(2));
}

// ---------- issue_636 ----------

#[test]
fn plan_filter_case_insensitive_column_ref() {
    let spark = spark();
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let rows = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let plan_steps = vec![json!({
        "op": "filter",
        "payload": {"op": "gt", "left": {"col": "ID"}, "right": {"lit": 1}}
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("id").and_then(|v| v.as_i64()), Some(2));
}

#[test]
fn plan_select_case_insensitive_column_name() {
    let spark = spark();
    let schema = vec![("Name".to_string(), "string".to_string())];
    let rows = vec![vec![json!("Alice")]];
    let plan_steps = vec![json!({
        "op": "select",
        "payload": ["name"]
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("Name").and_then(|v| v.as_str()), Some("Alice"));
}

// ---------- issue_548 ----------

#[test]
fn issue_548_plan_op_to_date() {
    let session = spark();
    let data = vec![vec![json!("2024-03-15")]];
    let schema = vec![("d".to_string(), "string".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "dt",
            "expr": {"op": "to_date", "args": [{"col": "d"}]}
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains_key("dt"));
}

#[test]
fn issue_548_plan_op_date_trunc() {
    let session = spark();
    let data = vec![vec![json!("2024-03-15 12:30:45")]];
    let schema = vec![("ts".to_string(), "string".to_string())];
    let plan_ops = vec![
        json!({
            "op": "withColumn",
            "payload": {
                "name": "dt",
                "expr": {"op": "to_date", "args": [{"col": "ts"}]}
            }
        }),
        json!({
            "op": "withColumn",
            "payload": {
                "name": "truncated",
                "expr": {"op": "date_trunc", "args": [{"lit": "1d"}, {"col": "dt"}]}
            }
        }),
    ];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains_key("truncated"));
}
