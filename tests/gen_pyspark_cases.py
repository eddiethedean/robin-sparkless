"""
Generate PySpark parity fixtures for robin-sparkless tests.

This script is not used by the Rust build; it is a helper to produce
`tests/fixtures/*.json` files that encode:

- Input schema + rows
- A sequence of PySpark-style operations
- Expected schema + rows

See `TEST_CREATION_GUIDE.md` for the full workflow.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import SparkSession


def schema_to_json(schema) -> List[Dict[str, Any]]:
    return [
        {"name": f.name, "type": f.dataType.simpleString()}
        for f in schema.fields
    ]


def df_to_rows(df) -> List[List[Any]]:
    return [list(r) for r in df.collect()]


def case_filter_age_gt_30(spark: SparkSession) -> Dict[str, Any]:
    data = [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")]
    df = spark.createDataFrame(data, ["id", "age", "name"])

    out_df = df.filter("age > 30").select("name", "age").orderBy("name")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "filter_age_gt_30",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "filter", "expr": "col('age') > 30"},
            {"op": "select", "columns": ["name", "age"]},
            {"op": "orderBy", "columns": ["name"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_groupby_count(spark: SparkSession) -> Dict[str, Any]:
    data = [
        (1, "Alice", "Sales"),
        (2, "Bob", "Sales"),
        (3, "Charlie", "Engineering"),
        (4, "David", "Engineering"),
        (5, "Eve", "Sales"),
    ]
    df = spark.createDataFrame(data, ["id", "name", "department"])

    out_df = df.groupBy("department").count().orderBy("department")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "groupby_count",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "groupBy", "columns": ["department"]},
            {"op": "agg", "aggregations": [{"func": "count", "alias": "count"}]},
            {"op": "orderBy", "columns": ["department"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_groupby_with_nulls(spark: SparkSession) -> Dict[str, Any]:
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("category", StringType(), True),
        ]
    )
    data = [(1, "A"), (2, "A"), (3, None), (4, "B"), (5, None)]
    df = spark.createDataFrame(data, schema=schema)

    out_df = df.groupBy("category").count().orderBy("category")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "groupby_with_nulls",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "groupBy", "columns": ["category"]},
            {"op": "agg", "aggregations": [{"func": "count", "alias": "count"}]},
            {"op": "orderBy", "columns": ["category"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def main() -> None:
    spark = SparkSession.builder.appName("robin_sparkless_parity_gen").getOrCreate()

    out_dir = Path("tests/fixtures")
    out_dir.mkdir(parents=True, exist_ok=True)

    fixtures: List[Dict[str, Any]] = [
        case_filter_age_gt_30(spark),
        case_groupby_count(spark),
        case_groupby_with_nulls(spark),
    ]

    for fx in fixtures:
        path = out_dir / f"{fx['name']}.json"
        path.write_text(json.dumps(fx, indent=2))
        print(f"wrote {path}")

    spark.stop()


if __name__ == "__main__":
    main()

