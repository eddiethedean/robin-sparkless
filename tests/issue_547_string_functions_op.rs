//! Regression tests for issue #547 – String functions as op (translate, substring_index,
//! levenshtein, soundex, crc32, xxhash64, get_json_object, json_tuple, regexp_extract_all).
//!
//! Plan interpreter now accepts these as {"op": "<name>", "args": [...]}.

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

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
    assert!(
        rows[0].get("m").is_some(),
        "regexp_extract_all should produce column m"
    );
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
