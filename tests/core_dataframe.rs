//! Core DataFrame API, filter, select, join, and lazy execution.
//!
//! Merged from: dataframe_core, issue_401, issue_639, issue_552, issue_558,
//! issue_644_645_646, lazy_backend.

mod common;

use common::{small_people_df, spark};
use polars::prelude::{col as pl_col, df, len, lit as pl_lit};
use robin_sparkless::dataframe::SelectItem;
use robin_sparkless::functions::{col, lit_i64};
use robin_sparkless::plan;
use robin_sparkless::{DataFrame, JoinType};
use serde_json::Value as JsonValue;
use serde_json::json;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};

fn sample_df() -> DataFrame {
    spark()
        .create_dataframe(
            vec![
                (1i64, 25i64, "Alice".to_string()),
                (2i64, 30i64, "Bob".to_string()),
                (3i64, 25i64, "Carol".to_string()),
                (4i64, 35i64, "Dave".to_string()),
                (5i64, 30i64, "Eve".to_string()),
            ],
            vec!["id", "age", "name"],
        )
        .unwrap()
}

// ---------- dataframe_core ----------

/// Equivalent to `test_create_dataframe_and_collect` in
/// `tests/python/test_robin_sparkless.py`.
#[test]
fn create_dataframe_and_collect_core() {
    let df = small_people_df();
    let n = df.count().unwrap();
    assert_eq!(n, 3);

    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 1);
    assert_eq!(rows[0]["age"].as_i64().unwrap(), 25);
    assert_eq!(rows[0]["name"].as_str().unwrap(), "Alice");
}

/// Equivalent to `test_filter_and_select` from the Python test suite:
/// filter on a numeric predicate then select a subset of columns.
#[test]
fn filter_and_select_core() {
    let df = small_people_df();

    // filter: age > 28
    let expr = col("age").gt(lit_i64(28).into_expr()).into_expr();
    let filtered = df.filter(expr).unwrap();
    assert_eq!(filtered.count().unwrap(), 2);

    let filtered_rows = filtered.collect_as_json_rows().unwrap();
    assert!(
        filtered_rows
            .iter()
            .all(|r| r["age"].as_i64().unwrap() > 28)
    );

    // select columns
    let selected = df.select(vec!["id", "name"]).unwrap();
    assert_eq!(selected.count().unwrap(), 3);
    let selected_rows = selected.collect_as_json_rows().unwrap();
    let first = &selected_rows[0];
    assert!(first.get("id").is_some());
    assert!(first.get("name").is_some());
    assert!(first.get("age").is_none());
}

/// Column–column comparison semantics, adapted from
/// `test_filter_column_vs_column` and related tests in the Python suite.
#[test]
fn filter_column_vs_column_core() {
    let spark = spark();
    let pl = df![
        "a" => &[1i64, 2i64, 3i64, 4i64, 5i64],
        "b" => &[5i64, 4i64, 1i64, 2i64, 1i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // a > b via method-style comparison
    let gt_rows = df
        .filter(col("a").gt(col("b").into_expr()).into_expr())
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(gt_rows.len(), 3);

    // Other operators: <, >=, <=, ==, != (check row counts)
    assert_eq!(
        df.filter(col("a").lt(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        2
    );
    assert_eq!(
        df.filter(col("a").gt_eq(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        3
    );
    assert_eq!(
        df.filter(col("a").lt_eq(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        2
    );
    assert_eq!(
        df.filter(col("a").eq(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        0
    );
    assert_eq!(
        df.filter(col("a").neq(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        5
    );
}

/// Column–column comparison combined with literals, mirroring
/// `test_filter_column_vs_column_combined_with_literal` in Python.
#[test]
fn filter_column_vs_column_combined_with_literal_core() {
    let spark = spark();
    let pl = df![
        "a" => &[1i64, 2i64, 3i64, 4i64, 5i64],
        "b" => &[5i64, 4i64, 1i64, 2i64, 1i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // Use Polars expressions to mirror (a > b) & (a > 2)
    let expr_and = pl_col("a")
        .gt(pl_col("b"))
        .and(pl_col("a").gt(pl_lit(2_i64)));
    let out = df.filter(expr_and).unwrap().collect_as_json_rows().unwrap();
    assert_eq!(out.len(), 3);

    // (a < b) | (b >= 5)
    let expr_or = pl_col("a")
        .lt(pl_col("b"))
        .or(pl_col("b").gt_eq(pl_lit(5_i64)));
    let out2 = df.filter(expr_or).unwrap().collect_as_json_rows().unwrap();
    assert_eq!(out2.len(), 2);
}

/// Basic join semantics on small in-memory DataFrames, mirroring
/// simple PySpark pipelines that join on a single key.
#[test]
fn join_inner_and_left_core() {
    let spark = spark();
    let left_pl = df![
        "id" => &[1i64, 2i64, 3i64],
        "v"  => &[10i64, 20i64, 30i64],
    ]
    .unwrap();
    let right_pl = df![
        "id" => &[2i64, 3i64, 4i64],
        "w"  => &[200i64, 300i64, 400i64],
    ]
    .unwrap();

    let left = spark.create_dataframe_from_polars(left_pl);
    let right = spark.create_dataframe_from_polars(right_pl);

    // Inner join: ids 2 and 3 are present on both sides.
    let inner = left
        .join(&right, vec!["id"], JoinType::Inner)
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let inner_ids: Vec<i64> = inner.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(inner_ids, vec![2, 3]);

    // Left join: ids 1, 2, 3 from the left; id 1 has null `w`.
    let left_join = spark
        .create_dataframe_from_polars(
            df![
                "id" => &[1i64, 2i64, 3i64],
                "v"  => &[10i64, 20i64, 30i64],
            ]
            .unwrap(),
        )
        .join(&right, vec!["id"], JoinType::Left)
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(left_join.len(), 3);
    assert_eq!(left_join[0]["id"].as_i64().unwrap(), 1);
    assert!(left_join[0]["w"].is_null());
}

/// Window functions via `with_column` on a small DataFrame: row_number
/// partitioned by a grouping column.
#[test]
fn with_column_window_row_number_core() {
    let spark = spark();
    let pl = df![
        "dept" => &["A", "A", "B", "B"],
        "salary" => &[100i64, 200i64, 150i64, 250i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let with_rn = df
        .with_column("rn", &col("salary").row_number(true).over(&["dept"]))
        .unwrap();
    let rows = with_rn.collect_as_json_rows().unwrap();

    // Within each department, row numbers start at 1 and increase.
    let mut a_ranks = Vec::new();
    let mut b_ranks = Vec::new();
    for row in rows {
        let dept = row["dept"].as_str().unwrap();
        let rn = row["rn"].as_i64().unwrap();
        match dept {
            "A" => a_ranks.push(rn),
            "B" => b_ranks.push(rn),
            other => panic!("unexpected dept {other}"),
        }
    }
    a_ranks.sort();
    b_ranks.sort();
    assert_eq!(a_ranks, vec![1, 2]);
    assert_eq!(b_ranks, vec![1, 2]);
}

/// Simple with_column string transformation: uppercasing a name field.
#[test]
fn with_column_string_upper_core() {
    let df = small_people_df();
    let df2 = df.with_column("name_upper", &col("name").upper()).unwrap();
    let rows = df2.collect_as_json_rows().unwrap();

    let names: Vec<&str> = rows
        .iter()
        .map(|r| r["name_upper"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"ALICE"));
    assert!(names.contains(&"BOB"));
    assert!(names.contains(&"CAROL"));
}

// ---------- issue_401 ----------

#[test]
fn issue_401_filter_with_boolean_column_expression() {
    let spark = spark();
    let pl = df![
        "id" => &[1i64, 2i64, 3i64],
        "v" => &[10i64, 20i64, 30i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // Build a boolean expression column and use it in filter.
    let expr = col("v").gt_eq(lit_i64(20).into_expr()).into_expr();
    let filtered = df.filter(expr).unwrap();
    let rows = filtered.collect_as_json_rows().unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 2);
    assert_eq!(rows[1]["id"].as_i64().unwrap(), 3);
}

// ---------- issue_639 ----------

/// #639: inner join row count — only matching rows.
#[test]
fn plan_join_inner_row_count() {
    let spark = spark();
    let left_data = vec![
        vec![json!(1), json!(10)],
        vec![json!(2), json!(20)],
        vec![json!(3), json!(10)],
    ];
    let left_schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("fk".to_string(), "bigint".to_string()),
    ];
    let other_schema = vec![
        json!({"name": "fk", "type": "long"}),
        json!({"name": "label", "type": "string"}),
    ];
    let plan_steps = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[10, "A"], [20, "B"]],
            "other_schema": other_schema,
            "on": ["fk"],
            "how": "inner"
        }
    })];
    let df = plan::execute_plan(&spark, left_data, left_schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 3, "inner join: 3 left rows match (10,20,10)");
}

/// #639: left join row count — all left rows preserved.
#[test]
fn plan_join_left_row_count() {
    let spark = spark();
    let left_data = vec![vec![json!(1), json!(10)], vec![json!(2), json!(99)]];
    let left_schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("fk".to_string(), "bigint".to_string()),
    ];
    let other_schema = vec![
        json!({"name": "fk", "type": "long"}),
        json!({"name": "label", "type": "string"}),
    ];
    let plan_steps = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[10, "X"]],
            "other_schema": other_schema,
            "on": ["fk"],
            "how": "left"
        }
    })];
    let df = plan::execute_plan(&spark, left_data, left_schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(
        out.len(),
        2,
        "left join: 2 left rows (fk=99 has null right)"
    );
}

// ---------- issue_552 ----------

/// Emp 4 rows (dept_id 10,20,10,30), dept 3 rows (10,20,40). Inner on dept_id -> 3 rows, left -> 4 rows.
#[test]
fn issue_552_plan_join_inner_and_left_row_counts() {
    let session = spark();
    // Left: emp (id, name, dept_id)
    let emp_data = vec![
        vec![json!(1), json!("Alice"), json!(10)],
        vec![json!(2), json!("Bob"), json!(20)],
        vec![json!(3), json!("Charlie"), json!(10)],
        vec![json!(4), json!("David"), json!(30)],
    ];
    let emp_schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
        ("dept_id".to_string(), "bigint".to_string()),
    ];
    let dept_schema_plan = vec![
        json!({"name": "dept_id", "type": "long"}),
        json!({"name": "name", "type": "string"}),
    ];

    let plan_inner = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[10, "IT"], [20, "HR"], [40, "Finance"]],
            "other_schema": dept_schema_plan,
            "on": ["dept_id"],
            "how": "inner"
        }
    })];
    let df_inner =
        plan::execute_plan(&session, emp_data.clone(), emp_schema.clone(), &plan_inner).unwrap();
    let rows_inner = df_inner.collect_as_json_rows().unwrap();
    assert_eq!(
        rows_inner.len(),
        3,
        "inner join must return 3 rows (dept_id 10,20,10)"
    );

    let plan_left = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[10, "IT"], [20, "HR"], [40, "Finance"]],
            "other_schema": dept_schema_plan,
            "on": ["dept_id"],
            "how": "left"
        }
    })];
    let df_left = plan::execute_plan(&session, emp_data, emp_schema, &plan_left).unwrap();
    let rows_left = df_left.collect_as_json_rows().unwrap();
    assert_eq!(rows_left.len(), 4, "left join must return 4 rows");
}

// ---------- issue_558 ----------

#[test]
fn issue_558_join_string_and_long_coerces() {
    let session = spark();
    // Left: id as string "1", "2"
    let left_data = vec![vec![json!("1"), json!("a")], vec![json!("2"), json!("b")]];
    let left_schema = vec![
        ("id".to_string(), "string".to_string()),
        ("label".to_string(), "string".to_string()),
    ];
    // Right: id as long 1, 2
    let right_schema_plan = vec![
        json!({"name": "id", "type": "long"}),
        json!({"name": "name", "type": "string"}),
    ];
    let plan_ops = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[1, "x"], [2, "y"]],
            "other_schema": right_schema_plan,
            "on": ["id"],
            "how": "inner"
        }
    })];
    let df = plan::execute_plan(&session, left_data, left_schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(
        rows.len(),
        2,
        "inner join on id: string 1,2 vs long 1,2 -> 2 rows"
    );
}

// ---------- issue_644_645_646 ----------

#[test]
fn test_plan_filter_with_bare_string_column_ref() {
    // #644: plan "filter" with payload as bare column name (string) should be accepted as column ref.
    let spark = spark();
    let rows = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let plan_steps = vec![
        json!({"op": "filter", "payload": "id"}), // bare string = column ref (non-boolean will fail at collect or we coerce)
    ];
    let result = plan::execute_plan(&spark, rows, schema, &plan_steps);
    if let Err(e) = &result {
        let msg = e.to_string();
        assert!(
            !msg.contains("expression must be a JSON object"),
            "plan should accept bare string as column ref, got: {}",
            msg
        );
    }
}

#[test]
fn test_plan_filter_with_object_column_ref() {
    // Filter with proper boolean expression.
    let spark = spark();
    let rows = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let plan_steps = vec![json!({
        "op": "filter",
        "payload": {"op": "gt", "left": {"col": "id"}, "right": {"lit": 1}}
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("id").and_then(|v| v.as_i64()), Some(2));
}

#[test]
fn test_select_items_mixed_names_and_exprs() {
    // #645: select with mix of column names and expressions.
    let spark = spark();
    let rows = vec![vec![json!(1), json!(10)], vec![json!(2), json!(20)]];
    let schema = vec![
        ("a".to_string(), "bigint".to_string()),
        ("b".to_string(), "bigint".to_string()),
    ];
    let df = spark
        .create_dataframe_from_rows_engine(rows, schema)
        .unwrap();
    let items = vec![
        SelectItem::ColumnName("a"),
        SelectItem::Expr(col("b").alias("b_doubled").into_expr()),
    ];
    let out = df.select_items(items).unwrap();
    let names = out.columns_engine().unwrap();
    assert_eq!(names, vec!["a", "b_doubled"]);
    let rows = out.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("a").and_then(|v: &JsonValue| v.as_i64()),
        Some(1)
    );
    assert_eq!(
        rows[0]
            .get("b_doubled")
            .and_then(|v: &JsonValue| v.as_i64()),
        Some(10)
    );
}

// ---------- lazy_backend ----------

#[test]
fn lazy_schema_resolution_before_collect() {
    let df = sample_df();

    let schema = df.schema().unwrap();
    assert_eq!(schema.fields().len(), 3);

    let cols = df.columns().unwrap();
    assert_eq!(cols, vec!["id", "age", "name"]);

    let resolved = df.resolve_column_name("AGE").unwrap();
    assert_eq!(resolved, "age");

    let dtype = df.get_column_dtype("id").unwrap();
    assert!(dtype.is_integer());
}

#[test]
fn lazy_full_pipeline_filter_select_groupby_agg() {
    let df = sample_df();

    let result = df
        .filter(pl_col("age").gt_eq(pl_lit(28)))
        .unwrap()
        .select(vec!["id", "age", "name"])
        .unwrap()
        .group_by(vec!["age"])
        .unwrap()
        .agg(vec![len().alias("count")])
        .unwrap();

    let rows = result.collect().unwrap();
    assert_eq!(rows.height(), 2);
    let count_col = rows.column("count").unwrap();
    let counts: Vec<u32> = count_col
        .u32()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap_or(0))
        .collect();
    assert_eq!(counts.iter().sum::<u32>(), 3);
}

#[test]
fn lazy_transformation_chain_no_intermediate_collect() {
    let df = sample_df();

    let filtered = df.filter(pl_col("age").eq(pl_lit(25))).unwrap();
    assert_eq!(filtered.count().unwrap(), 2);

    let selected = filtered.select(vec!["id", "name"]).unwrap();
    assert_eq!(selected.count().unwrap(), 2);

    let limited = selected.limit(1).unwrap();
    assert_eq!(limited.count().unwrap(), 1);
}

#[test]
fn lazy_join_returns_lazy() {
    let left_pl = polars::prelude::df!("id" => &[1i64, 2i64], "label" => &["a", "b"]).unwrap();
    let right_pl = polars::prelude::df!("id" => &[1i64, 3i64], "value" => &[10i64, 30i64]).unwrap();
    let left = spark().create_dataframe_from_polars(left_pl);
    let right = spark().create_dataframe_from_polars(right_pl);

    let joined = left.join(&right, vec!["id"], JoinType::Inner).unwrap();
    assert_eq!(joined.count().unwrap(), 1);

    let left_joined = left.join(&right, vec!["id"], JoinType::Left).unwrap();
    assert_eq!(left_joined.count().unwrap(), 2);
}

#[test]
fn lazy_union_returns_lazy() {
    let df1_pl = polars::prelude::df!("id" => &[1i64], "name" => &["a"]).unwrap();
    let df2_pl = polars::prelude::df!("id" => &[2i64], "name" => &["b"]).unwrap();
    let df1 = spark().create_dataframe_from_polars(df1_pl);
    let df2 = spark().create_dataframe_from_polars(df2_pl);

    let united = df1.union(&df2).unwrap();
    assert_eq!(united.count().unwrap(), 2);
}

#[test]
fn lazy_read_csv_returns_lazy() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "x,y").unwrap();
    writeln!(f, "1,10").unwrap();
    writeln!(f, "2,20").unwrap();
    f.flush().unwrap();

    let df = spark().read_csv(f.path()).unwrap();

    let cols = df.columns().unwrap();
    assert_eq!(cols, vec!["x", "y"]);

    assert_eq!(df.count().unwrap(), 2);
}

#[test]
fn lazy_read_parquet_returns_lazy() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data.parquet");

    let df_in_pl = polars::prelude::df!("id" => &[1i64], "name" => &["a"]).unwrap();
    let df_in = spark().create_dataframe_from_polars(df_in_pl);
    df_in
        .write()
        .mode(robin_sparkless::WriteMode::Overwrite)
        .format(robin_sparkless::WriteFormat::Parquet)
        .save(&path)
        .unwrap();

    let df = spark().read_parquet(&path).unwrap();
    assert_eq!(df.columns().unwrap(), vec!["id", "name"]);
    assert_eq!(df.count().unwrap(), 1);
}

#[test]
fn lazy_distinct_drop_dropna_fillna() {
    let df = sample_df();

    let distinct = df.distinct(Some(vec!["age"])).unwrap();
    assert_eq!(distinct.count().unwrap(), 3);

    let dropped = df.drop(vec!["name"]).unwrap();
    assert_eq!(dropped.columns().unwrap(), vec!["id", "age"]);
    assert_eq!(dropped.count().unwrap(), 5);
}

#[test]
fn lazy_limit_offset() {
    let df = sample_df();

    let limited = df.limit(2).unwrap();
    assert_eq!(limited.count().unwrap(), 2);

    let offset = df.offset(2).unwrap();
    assert_eq!(offset.count().unwrap(), 3);
}

#[test]
fn lazy_actions_materialize() {
    let df = sample_df();

    assert_eq!(df.count().unwrap(), 5);

    let rows = df.collect().unwrap();
    assert_eq!(rows.height(), 5);

    let json_rows = df.collect_as_json_rows().unwrap();
    assert_eq!(json_rows.len(), 5);
}

#[test]
fn lazy_create_dataframe_is_lazy() {
    let df = spark()
        .create_dataframe(
            vec![
                (1i64, 10i64, "x".to_string()),
                (2i64, 20i64, "y".to_string()),
            ],
            vec!["a", "b", "c"],
        )
        .unwrap();

    let cols = df.columns().unwrap();
    assert_eq!(cols, vec!["a", "b", "c"]);
    assert_eq!(df.count().unwrap(), 2);
}

#[test]
fn lazy_range_is_lazy() {
    let df = spark().range(0, 10, 1).unwrap();
    assert_eq!(df.columns().unwrap(), vec!["id"]);
    assert_eq!(df.count().unwrap(), 10);
}

#[test]
fn lazy_pivot_and_groupby_agg() {
    let df = sample_df();

    let grouped = df.group_by(vec!["age"]).unwrap();
    let counted = grouped.count().unwrap();
    assert_eq!(counted.count().unwrap(), 3);

    let summed = df.group_by(vec!["age"]).unwrap().sum("id").unwrap();
    assert_eq!(summed.count().unwrap(), 3);
}
