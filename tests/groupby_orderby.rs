//! groupBy, orderBy, sort nulls, first/last, between/power.
//!
//! Merged from: groupby_orderby_core, issue_492, issue_540, issue_560, issue_640_641.

mod common;

use common::spark;
use polars::prelude::df;
use robin_sparkless::functions::{col, desc_nulls_last};
use robin_sparkless::plan;
use serde_json::json;

fn expected_values_asc() -> Vec<i64> {
    vec![5, 10, 20]
}

// ---------- groupby_orderby_core ----------

#[test]
fn group_by_column_then_agg_sum_core() {
    let spark = spark();
    let pl = df![
        "dept" => &["A", "A", "B"],
        "salary" => &[100i64, 200i64, 150i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let gd = df.group_by(vec!["dept"]).unwrap();
    let out_df = gd.sum("salary").unwrap();
    let out = out_df.collect_as_json_rows().unwrap();

    assert_eq!(out.len(), 2);
    let mut a_total = None;
    let mut b_total = None;
    for row in out {
        match row["dept"].as_str().unwrap() {
            "A" => a_total = Some(row["sum(salary)"].as_i64().unwrap()),
            "B" => b_total = Some(row["sum(salary)"].as_i64().unwrap()),
            other => panic!("unexpected dept {other}"),
        }
    }
    assert_eq!(a_total, Some(300));
    assert_eq!(b_total, Some(150));
}

#[test]
fn group_by_list_of_columns_core() {
    let spark = spark();
    let pl = df![
        "a" => &[1i64, 1i64, 1i64],
        "b" => &[10i64, 10i64, 20i64],
        "v" => &[100i64, 200i64, 50i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let gd = df.group_by(vec!["a", "b"]).unwrap();
    let out_df = gd.sum("v").unwrap();
    let out = out_df.collect_as_json_rows().unwrap();

    assert_eq!(out.len(), 2);
    let mut totals = std::collections::HashMap::new();
    for row in out {
        let key = (row["a"].as_i64().unwrap(), row["b"].as_i64().unwrap());
        let total = row["sum(v)"].as_i64().unwrap();
        totals.insert(key, total);
    }
    assert_eq!(totals.get(&(1, 10)), Some(&300));
    assert_eq!(totals.get(&(1, 20)), Some(&50));
}

#[test]
fn group_by_single_str_and_list_core() {
    let spark = spark();
    let pl = df![
        "dept" => &["A", "A"],
        "n" => &[1i64, 2i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let gd1 = df.group_by(vec!["dept"]).unwrap();
    let out1_df = gd1.sum("n").unwrap();
    let out1 = out1_df.collect_as_json_rows().unwrap();
    assert_eq!(out1.len(), 1);
    assert_eq!(out1[0]["dept"].as_str().unwrap(), "A");
    assert_eq!(out1[0]["sum(n)"].as_i64().unwrap(), 3);

    let gd2 = df.group_by(vec!["dept"]).unwrap();
    let out2_df = gd2.sum("n").unwrap();
    let out2 = out2_df.collect_as_json_rows().unwrap();
    assert_eq!(out2.len(), 1);
    assert_eq!(out2[0]["dept"].as_str().unwrap(), "A");
    assert_eq!(out2[0]["sum(n)"].as_i64().unwrap(), 3);
}

#[test]
fn order_by_column_and_list_core() {
    let spark = spark();
    let pl = df!["x" => &[3i64, 1i64, 2i64]].unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let out = df
        .order_by(vec!["x"], vec![true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let xs: Vec<i64> = out.iter().map(|r| r["x"].as_i64().unwrap()).collect();
    assert_eq!(xs, vec![1, 2, 3]);

    let pl2 = df![
        "a" => &[2i64, 1i64, 1i64],
        "b" => &[1i64, 2i64, 1i64],
    ]
    .unwrap();
    let df2 = spark.create_dataframe_from_polars(pl2);
    let out2 = df2
        .order_by(vec!["a", "b"], vec![true, true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(
        (
            out2[0]["a"].as_i64().unwrap(),
            out2[0]["b"].as_i64().unwrap()
        ),
        (1, 1)
    );
    assert_eq!(
        (
            out2[1]["a"].as_i64().unwrap(),
            out2[1]["b"].as_i64().unwrap()
        ),
        (1, 2)
    );
    assert_eq!(
        (
            out2[2]["a"].as_i64().unwrap(),
            out2[2]["b"].as_i64().unwrap()
        ),
        (2, 1)
    );
}

#[test]
fn group_by_with_null_keys_core() {
    let spark = spark();
    let pl = df![
        "dept" => &[Some("A"), Some("A"), None],
        "salary" => &[100i64, 200i64, 300i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let gd = df.group_by(vec!["dept"]).unwrap();
    let out_df = gd.sum("salary").unwrap();
    let out = out_df.collect_as_json_rows().unwrap();

    assert_eq!(out.len(), 2);
    let mut saw_a = false;
    let mut saw_null = false;
    for row in out {
        if row["dept"].is_null() {
            assert_eq!(row["sum(salary)"].as_i64().unwrap(), 300);
            saw_null = true;
        } else {
            assert_eq!(row["dept"].as_str().unwrap(), "A");
            assert_eq!(row["sum(salary)"].as_i64().unwrap(), 300);
            saw_a = true;
        }
    }
    assert!(saw_a && saw_null);
}

// ---------- issue_492 ----------

#[test]
fn issue_492_order_by_exact_case() {
    let spark = spark();
    let pl = df![
        "Name" => &["Alice", "Bob", "Charlie"],
        "Value" => &[10i64, 5i64, 20i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let rows = df
        .order_by(vec!["Value"], vec![true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let values: Vec<i64> = rows.iter().map(|r| r["Value"].as_i64().unwrap()).collect();
    assert_eq!(
        values,
        expected_values_asc(),
        "order_by(\"Value\") should sort ascending"
    );
}

#[test]
fn issue_492_order_by_lowercase() {
    let spark = spark();
    let pl = df![
        "Name" => &["Alice", "Bob", "Charlie"],
        "Value" => &[10i64, 5i64, 20i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let rows = df
        .order_by(vec!["value"], vec![true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let values: Vec<i64> = rows.iter().map(|r| r["Value"].as_i64().unwrap()).collect();
    assert_eq!(
        values,
        expected_values_asc(),
        "order_by(\"value\") should resolve to Value and sort ascending"
    );
}

#[test]
fn issue_492_order_by_uppercase() {
    let spark = spark();
    let pl = df![
        "Name" => &["Alice", "Bob", "Charlie"],
        "Value" => &[10i64, 5i64, 20i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let rows = df
        .order_by(vec!["VALUE"], vec![true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let values: Vec<i64> = rows.iter().map(|r| r["Value"].as_i64().unwrap()).collect();
    assert_eq!(
        values,
        expected_values_asc(),
        "order_by(\"VALUE\") should resolve to Value and sort ascending"
    );
}

// ---------- issue_540 ----------

#[test]
fn issue_540_desc_nulls_last() {
    let spark = spark();
    let pl = polars::prelude::df![
        "value" => &["A", "B", "C", "D"],
        "ord" => &[Some(1i64), Some(2i64), None, Some(4i64)],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let sorted = df
        .order_by_exprs(vec![desc_nulls_last(&col("ord"))])
        .unwrap();
    let rows = sorted.collect_as_json_rows().unwrap();
    let ords: Vec<Option<i64>> = rows
        .iter()
        .map(|r| r.get("ord").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(ords, vec![Some(4), Some(2), Some(1), None], "nulls last");
}

#[test]
fn issue_540_default_order_by_desc_null_order() {
    let spark = spark();
    let pl = polars::prelude::df![
        "value" => &["A", "B", "C", "D"],
        "ord" => &[Some(1i64), Some(2i64), None, Some(4i64)],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let sorted = df.order_by(vec!["ord"], vec![false]).unwrap();
    let rows = sorted.collect_as_json_rows().unwrap();
    let ords: Vec<Option<i64>> = rows
        .iter()
        .map(|r| r.get("ord").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(
        ords,
        vec![Some(4), Some(2), Some(1), None],
        "default DESC nulls last"
    );
}

// ---------- issue_560 ----------

#[test]
fn issue_560_groupby_first_last_then_orderby() {
    let session = spark();
    let data = vec![
        vec![json!("Sales"), json!("Alice"), json!(1000)],
        vec![json!("Sales"), json!("Bob"), json!(1500)],
        vec![json!("Eng"), json!("Charlie"), json!(2000)],
        vec![json!("Eng"), json!("Dave"), json!(2500)],
    ];
    let schema = vec![
        ("dept".to_string(), "string".to_string()),
        ("name".to_string(), "string".to_string()),
        ("salary".to_string(), "bigint".to_string()),
    ];
    let plan_ops = vec![
        json!({
            "op": "groupBy",
            "payload": {
                "group_by": ["dept"],
                "aggs": [
                    {"agg": "first", "column": "name", "alias": "first(name)"},
                    {"agg": "last", "column": "name", "alias": "last(name)"}
                ]
            }
        }),
        json!({
            "op": "orderBy",
            "payload": { "columns": ["dept"], "ascending": [true] }
        }),
    ];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2, "2 groups: Eng, Sales");
    let first_row = &rows[0];
    let second_row = &rows[1];
    assert_eq!(first_row.get("dept").and_then(|v| v.as_str()), Some("Eng"));
    assert_eq!(
        second_row.get("dept").and_then(|v| v.as_str()),
        Some("Sales")
    );
    assert!(first_row.contains_key("first(name)"));
    assert!(first_row.contains_key("last(name)"));
}

// ---------- issue_640_641 ----------

#[test]
fn plan_filter_between() {
    let spark = spark();
    let schema = vec![
        ("a".to_string(), "bigint".to_string()),
        ("b".to_string(), "bigint".to_string()),
    ];
    let rows = vec![
        vec![json!(2), json!(10)],
        vec![json!(5), json!(20)],
        vec![json!(8), json!(30)],
    ];
    let plan_steps = vec![json!({
        "op": "filter",
        "payload": {"op": "between", "left": {"col": "a"}, "lower": {"lit": 3}, "upper": {"lit": 7}}
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("a").and_then(|v| v.as_i64()), Some(5));
    assert_eq!(out[0].get("b").and_then(|v| v.as_i64()), Some(20));
}

#[test]
fn plan_with_column_power_op() {
    let spark = spark();
    let schema = vec![("a".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(5)], vec![json!(10)]];
    let plan_steps = vec![json!({
        "op": "withColumn",
        "payload": {"name": "squared", "expr": {"op": "**", "left": {"col": "a"}, "right": {"lit": 2}}}
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("squared").and_then(|v| v.as_f64()), Some(25.0));
    assert_eq!(out[1].get("squared").and_then(|v| v.as_f64()), Some(100.0));
}

#[test]
fn plan_groupby_sum() {
    let spark = spark();
    let schema = vec![
        ("grp".to_string(), "string".to_string()),
        ("n".to_string(), "bigint".to_string()),
    ];
    let rows = vec![
        vec![json!("G1"), json!(10)],
        vec![json!("G1"), json!(20)],
        vec![json!("G2"), json!(30)],
    ];
    let plan_steps = vec![json!({
        "op": "groupBy",
        "payload": {
            "group_by": ["grp"],
            "aggs": [{"agg": "sum", "column": "n", "alias": "total"}]
        }
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    let g1 = out
        .iter()
        .find(|r| r.get("grp").and_then(|v| v.as_str()) == Some("G1"))
        .unwrap();
    let g2 = out
        .iter()
        .find(|r| r.get("grp").and_then(|v| v.as_str()) == Some("G2"))
        .unwrap();
    assert_eq!(g1.get("total").and_then(|v| v.as_i64()), Some(30));
    assert_eq!(g2.get("total").and_then(|v| v.as_i64()), Some(30));
}
