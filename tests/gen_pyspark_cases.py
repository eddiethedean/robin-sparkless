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
import tempfile
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


def case_read_csv(spark: SparkSession) -> Dict[str, Any]:
    """Test reading CSV file and applying operations."""
    # Create a temporary CSV file
    csv_content = "id,age,name\n1,25,Alice\n2,30,Bob\n3,35,Charlie\n"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        csv_path = f.name
    
    try:
        # Read CSV using PySpark
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        
        # Apply operations
        out_df = df.filter("age > 30").select("name", "age").orderBy("name")
        
        input_schema = schema_to_json(df.schema)
        input_rows = df_to_rows(df)
        
        expected_schema = schema_to_json(out_df.schema)
        expected_rows = df_to_rows(out_df)
        
        return {
            "name": "read_csv",
            "pyspark_version": spark.version,
            "input": {
                "schema": input_schema,
                "rows": input_rows,
                "file_source": {
                    "format": "csv",
                    "content": csv_content,
                },
            },
            "operations": [
                {"op": "filter", "expr": "col('age') > 30"},
                {"op": "select", "columns": ["name", "age"]},
                {"op": "orderBy", "columns": ["name"], "ascending": [True]},
            ],
            "expected": {"schema": expected_schema, "rows": expected_rows},
        }
    finally:
        # Clean up temp file
        Path(csv_path).unlink(missing_ok=True)


def case_read_parquet(spark: SparkSession) -> Dict[str, Any]:
    """Test reading Parquet file and applying operations."""
    # Create data and write to Parquet
    data = [(1, "Alice", "Sales"), (2, "Bob", "Engineering"), (3, "Charlie", "Sales")]
    df_temp = spark.createDataFrame(data, ["id", "name", "department"])
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        parquet_path = f.name
    
    try:
        # Write to Parquet
        df_temp.write.mode("overwrite").parquet(parquet_path)
        
        # Read Parquet using PySpark
        df = spark.read.parquet(parquet_path)
        
        # Apply operations
        out_df = df.filter("department == 'Sales'").select("name", "department").orderBy("name")
        
        input_schema = schema_to_json(df.schema)
        input_rows = df_to_rows(df)
        
        expected_schema = schema_to_json(out_df.schema)
        expected_rows = df_to_rows(out_df)
        
        # For Parquet, we'll embed the original data as CSV-like content for the fixture
        # (Parquet is binary, so we store the source data representation)
        parquet_content = "id,name,department\n1,Alice,Sales\n2,Bob,Engineering\n3,Charlie,Sales\n"
        
        return {
            "name": "read_parquet",
            "pyspark_version": spark.version,
            "input": {
                "schema": input_schema,
                "rows": input_rows,
                "file_source": {
                    "format": "parquet",
                    "content": parquet_content,  # Source data representation
                },
            },
            "operations": [
                {"op": "filter", "expr": "col('department') == 'Sales'"},
                {"op": "select", "columns": ["name", "department"]},
                {"op": "orderBy", "columns": ["name"], "ascending": [True]},
            ],
            "expected": {"schema": expected_schema, "rows": expected_rows},
        }
    finally:
        # Clean up temp file
        import shutil
        if Path(parquet_path).exists():
            if Path(parquet_path).is_dir():
                shutil.rmtree(parquet_path)
            else:
                Path(parquet_path).unlink(missing_ok=True)


def case_read_json(spark: SparkSession) -> Dict[str, Any]:
    """Test reading JSON file and applying operations."""
    # Create JSONL content (one JSON object per line)
    json_content = '{"id":1,"age":25,"name":"Alice"}\n{"id":2,"age":30,"name":"Bob"}\n{"id":3,"age":35,"name":"Charlie"}\n'
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(json_content)
        json_path = f.name
    
    try:
        # Read JSON using PySpark
        df = spark.read.option("multiLine", "false").json(json_path)
        
        # Apply operations
        out_df = df.filter("age > 30").select("name", "age").orderBy("name")
        
        input_schema = schema_to_json(df.schema)
        input_rows = df_to_rows(df)
        
        expected_schema = schema_to_json(out_df.schema)
        expected_rows = df_to_rows(out_df)
        
        return {
            "name": "read_json",
            "pyspark_version": spark.version,
            "input": {
                "schema": input_schema,
                "rows": input_rows,
                "file_source": {
                    "format": "json",
                    "content": json_content,
                },
            },
            "operations": [
                {"op": "filter", "expr": "col('age') > 30"},
                {"op": "select", "columns": ["name", "age"]},
                {"op": "orderBy", "columns": ["name"], "ascending": [True]},
            ],
            "expected": {"schema": expected_schema, "rows": expected_rows},
        }
    finally:
        # Clean up temp file
        Path(json_path).unlink(missing_ok=True)


def case_when_otherwise(spark: SparkSession) -> Dict[str, Any]:
    """Test when().otherwise() conditional expression."""
    from pyspark.sql.functions import when as pyspark_when, col, lit
    
    data = [(1, 25), (2, 17), (3, 30), (4, 16)]
    df = spark.createDataFrame(data, ["id", "age"])
    
    # Use when(age >= 18).otherwise("minor") -> should return "adult" or "minor"
    out_df = df.withColumn(
        "status",
        pyspark_when(col("age") >= 18, lit("adult")).otherwise(lit("minor"))
    ).select("id", "age", "status").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "when_otherwise",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "status",
                "expr": "when(col('age') >= 18).then('adult').otherwise('minor')"
            },
            {"op": "select", "columns": ["id", "age", "status"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_when_then_otherwise(spark: SparkSession) -> Dict[str, Any]:
    """Test when().then().otherwise() conditional expression."""
    from pyspark.sql.functions import when as pyspark_when, col, lit
    
    data = [(1, 25), (2, 17), (3, 30), (4, 16)]
    df = spark.createDataFrame(data, ["id", "age"])
    
    # Use when(age >= 18).then("adult").otherwise("minor")
    out_df = df.withColumn(
        "status",
        pyspark_when(col("age") >= 18, lit("adult")).otherwise(lit("minor"))
    ).select("id", "age", "status").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "when_then_otherwise",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "status",
                "expr": "when(col('age') >= 18).then('adult').otherwise('minor')"
            },
            {"op": "select", "columns": ["id", "age", "status"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_coalesce(spark: SparkSession) -> Dict[str, Any]:
    """Test coalesce() function with nulls."""
    from pyspark.sql.functions import coalesce, col, lit
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
    ])
    data = [
        (1, "A", "X"),
        (2, None, "Y"),
        (3, "C", None),
        (4, None, None),
    ]
    df = spark.createDataFrame(data, schema=schema)
    
    # coalesce(col1, col2, lit("default"))
    out_df = df.withColumn(
        "result",
        coalesce(col("col1"), col("col2"), lit("default"))
    ).select("id", "col1", "col2", "result").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "coalesce",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "result",
                "expr": "coalesce(col('col1'), col('col2'), lit('default'))"
            },
            {"op": "select", "columns": ["id", "col1", "col2", "result"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_groupby_sum(spark: SparkSession) -> Dict[str, Any]:
    """Test groupBy().sum() aggregation."""
    data = [
        (1, "Sales", 1000),
        (2, "Sales", 1500),
        (3, "Engineering", 2000),
        (4, "Engineering", 2500),
        (5, "Sales", 1200),
    ]
    df = spark.createDataFrame(data, ["id", "department", "salary"])
    
    out_df = df.groupBy("department").sum("salary").orderBy("department")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "groupby_sum",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "groupBy", "columns": ["department"]},
            {"op": "agg", "aggregations": [{"func": "sum", "alias": "sum", "column": "salary"}]},
            {"op": "orderBy", "columns": ["department"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_groupby_avg(spark: SparkSession) -> Dict[str, Any]:
    """Test groupBy().avg() aggregation."""
    data = [
        (1, "Sales", 1000),
        (2, "Sales", 1500),
        (3, "Engineering", 2000),
        (4, "Engineering", 2500),
        (5, "Sales", 1200),
    ]
    df = spark.createDataFrame(data, ["id", "department", "salary"])
    
    out_df = df.groupBy("department").avg("salary").orderBy("department")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "groupby_avg",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "groupBy", "columns": ["department"]},
            {"op": "agg", "aggregations": [{"func": "avg", "alias": "avg", "column": "salary"}]},
            {"op": "orderBy", "columns": ["department"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_groupby_min(spark: SparkSession) -> Dict[str, Any]:
    """Test groupBy().min() aggregation."""
    data = [
        (1, "Sales", 1000),
        (2, "Sales", 1500),
        (3, "Engineering", 2000),
        (4, "Engineering", 2500),
        (5, "Sales", 1200),
    ]
    df = spark.createDataFrame(data, ["id", "department", "salary"])
    
    out_df = df.groupBy("department").min("salary").orderBy("department")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "groupby_min",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "groupBy", "columns": ["department"]},
            {"op": "agg", "aggregations": [{"func": "min", "alias": "min", "column": "salary"}]},
            {"op": "orderBy", "columns": ["department"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_groupby_max(spark: SparkSession) -> Dict[str, Any]:
    """Test groupBy().max() aggregation."""
    data = [
        (1, "Sales", 1000),
        (2, "Sales", 1500),
        (3, "Engineering", 2000),
        (4, "Engineering", 2500),
        (5, "Sales", 1200),
    ]
    df = spark.createDataFrame(data, ["id", "department", "salary"])
    
    out_df = df.groupBy("department").max("salary").orderBy("department")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "groupby_max",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "groupBy", "columns": ["department"]},
            {"op": "agg", "aggregations": [{"func": "max", "alias": "max", "column": "salary"}]},
            {"op": "orderBy", "columns": ["department"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_groupby_null_keys(spark: SparkSession) -> Dict[str, Any]:
    """Test groupBy with NULL values as grouping keys."""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )
    data = [
        (1, "A", 10),
        (2, "A", 20),
        (3, None, 30),
        (4, "B", 40),
        (5, None, 50),
        (6, "B", 60),
    ]
    df = spark.createDataFrame(data, schema=schema)

    out_df = df.groupBy("category").sum("value").orderBy("category")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "groupby_null_keys",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "groupBy", "columns": ["category"]},
            {"op": "agg", "aggregations": [{"func": "sum", "alias": "sum", "column": "value"}]},
            {"op": "orderBy", "columns": ["category"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_null_comparison_equality(spark: SparkSession) -> Dict[str, Any]:
    """Test null comparison semantics: col == NULL, col != NULL return NULL."""
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    from pyspark.sql.functions import col, lit, when
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True),
    ])
    data = [(1, 10), (2, None), (3, 20), (4, None)]
    df = spark.createDataFrame(data, schema=schema)
    
    # Test that col == NULL and col != NULL return NULL (not boolean)
    # Use a null column instead of lit(None) for Polars compatibility
    from pyspark.sql.functions import when as pyspark_when
    df_with_null_col = df.withColumn("null_col", lit(None).cast("int"))
    out_df = df_with_null_col.withColumn(
        "eq_null",
        pyspark_when(col("value") == col("null_col"), lit("NULL")).otherwise(lit("NOT_NULL"))
    ).withColumn(
        "ne_null",
        pyspark_when(col("value") != col("null_col"), lit("NOT_NULL")).otherwise(lit("NULL"))
    ).select("id", "value", "eq_null", "ne_null").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "null_comparison_equality",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "null_col",
                "expr": "lit(None)"
            },
            {
                "op": "withColumn",
                "column": "eq_null",
                "expr": "when(col('value') == col('null_col')).then('NULL').otherwise('NOT_NULL')"
            },
            {
                "op": "withColumn",
                "column": "ne_null",
                "expr": "when(col('value') != col('null_col')).then('NOT_NULL').otherwise('NULL')"
            },
            {"op": "select", "columns": ["id", "value", "eq_null", "ne_null"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_null_comparison_ordering(spark: SparkSession) -> Dict[str, Any]:
    """Test null comparison semantics: col > NULL, col < NULL return NULL."""
    from pyspark.sql.types import StructType, StructField, IntegerType
    from pyspark.sql.functions import col, lit, when
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True),
    ])
    data = [(1, 10), (2, None), (3, 20), (4, None)]
    df = spark.createDataFrame(data, schema=schema)
    
    # Test that ordering comparisons with NULL return NULL
    # Note: We test this indirectly by checking that comparisons with null columns return null
    # Direct lit(None) comparisons are not easily representable in Polars
    # Instead, we'll test by comparing with a null column
    from pyspark.sql.functions import when as pyspark_when
    # Create a column that's always null for comparison
    df_with_null_col = df.withColumn("null_col", lit(None).cast("int"))
    out_df = df_with_null_col.withColumn(
        "gt_null",
        pyspark_when(col("value") > col("null_col"), lit("TRUE")).otherwise(lit("FALSE_OR_NULL"))
    ).withColumn(
        "lt_null",
        pyspark_when(col("value") < col("null_col"), lit("TRUE")).otherwise(lit("FALSE_OR_NULL"))
    ).select("id", "value", "gt_null", "lt_null").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "null_comparison_ordering",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "null_col",
                "expr": "lit(None)"
            },
            {
                "op": "withColumn",
                "column": "gt_null",
                "expr": "when(col('value') > col('null_col')).then('TRUE').otherwise('FALSE_OR_NULL')"
            },
            {
                "op": "withColumn",
                "column": "lt_null",
                "expr": "when(col('value') < col('null_col')).then('TRUE').otherwise('FALSE_OR_NULL')"
            },
            {"op": "select", "columns": ["id", "value", "gt_null", "lt_null"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_null_safe_equality(spark: SparkSession) -> Dict[str, Any]:
    """Test null-safe equality: NULL <=> NULL returns True."""
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    from pyspark.sql.functions import col, lit
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value1", IntegerType(), True),
        StructField("value2", IntegerType(), True),
    ])
    data = [
        (1, 10, 10),
        (2, None, None),
        (3, 20, None),
        (4, None, 30),
        (5, 40, 40),
    ]
    df = spark.createDataFrame(data, schema=schema)
    
    # Test eqNullSafe: NULL <=> NULL = True, value <=> NULL = False
    out_df = df.withColumn(
        "null_safe_eq",
        col("value1").eqNullSafe(col("value2"))
    ).select("id", "value1", "value2", "null_safe_eq").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "null_safe_equality",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "null_safe_eq",
                "expr": "col('value1').eqNullSafe(col('value2'))"
            },
            {"op": "select", "columns": ["id", "value1", "value2", "null_safe_eq"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_null_in_filter(spark: SparkSession) -> Dict[str, Any]:
    """Test filtering with null values: df.filter(col != 1) excludes NULL rows."""
    from pyspark.sql.types import StructType, StructField, IntegerType
    from pyspark.sql.functions import col
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True),
    ])
    data = [(1, 1), (2, 2), (3, None), (4, 1), (5, None)]
    df = spark.createDataFrame(data, schema=schema)
    
    # filter(col != 1) should exclude rows where value is NULL
    # because NULL != 1 returns NULL (falsy)
    out_df = df.filter(col("value") != 1).select("id", "value").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "null_in_filter",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "filter", "expr": "col('value') != 1"},
            {"op": "select", "columns": ["id", "value"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_type_coercion_numeric(spark: SparkSession) -> Dict[str, Any]:
    """Test type coercion: int vs double comparisons."""
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
    from pyspark.sql.functions import col
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("int_col", IntegerType(), True),
        StructField("double_col", DoubleType(), True),
    ])
    data = [(1, 10, 5.5), (2, 20, 15.7), (3, 30, 25.3)]
    df = spark.createDataFrame(data, schema=schema)
    
    # Test that int_col > 5.5 coerces int to double
    out_df = df.filter(col("int_col") > 5.5).select("id", "int_col", "double_col").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "type_coercion_numeric",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "filter", "expr": "col('int_col') > 5.5"},
            {"op": "select", "columns": ["id", "int_col", "double_col"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_type_coercion_mixed(spark: SparkSession) -> Dict[str, Any]:
    """Test type coercion in arithmetic operations."""
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
    from pyspark.sql.functions import col
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("int_val", IntegerType(), True),
        StructField("double_val", DoubleType(), True),
    ])
    data = [(1, 10, 2.5), (2, 20, 3.5), (3, 30, 4.5)]
    df = spark.createDataFrame(data, schema=schema)
    
    # Test arithmetic with type coercion: int + double = double
    # We'll use withColumn to see the result type
    out_df = df.withColumn(
        "sum",
        col("int_val") + col("double_val")
    ).select("id", "int_val", "double_val", "sum").orderBy("id")
    
    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)
    
    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)
    
    return {
        "name": "type_coercion_mixed",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "sum",
                "expr": "col('int_val') + col('double_val')"
            },
            {"op": "select", "columns": ["id", "int_val", "double_val", "sum"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
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


def case_filter_and_or(spark: SparkSession) -> Dict[str, Any]:
    """Filter combining AND/OR conditions."""
    from pyspark.sql.types import StructType, StructField, IntegerType, IntegerType as IntType
    from pyspark.sql.functions import col

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("score", IntType(), True),
        ]
    )
    data = [
        (1, 25, 50),
        (2, 35, 60),
        (3, 45, 110),
        (4, 55, 70),
    ]
    df = spark.createDataFrame(data, schema=schema)

    out_df = df.filter(
        (col("age") > 30) & ((col("score") < 100) | (col("age") > 50))
    ).orderBy("id")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "filter_and_or",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "filter",
                "expr": "col('age') > 30 AND (col('score') < 100 OR col('age') > 50)",
            },
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_filter_not(spark: SparkSession) -> Dict[str, Any]:
    """Filter using NOT on a condition."""
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    from pyspark.sql.functions import col

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("flag", StringType(), True),
        ]
    )
    data = [
        (1, "Y"),
        (2, "N"),
        (3, "Y"),
        (4, "N"),
    ]
    df = spark.createDataFrame(data, schema=schema)

    out_df = df.filter(~(col("flag") == "N")).orderBy("id")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "filter_not",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "filter", "expr": "NOT col('flag') == 'N'"},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_with_logical_column(spark: SparkSession) -> Dict[str, Any]:
    """withColumn producing a boolean from a complex logical expression."""
    from pyspark.sql.types import StructType, StructField, IntegerType
    from pyspark.sql.functions import col

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("score", IntegerType(), True),
        ]
    )
    data = [
        (1, 25, 50),
        (2, 35, 60),
        (3, 45, 110),
        (4, 55, 70),
    ]
    df = spark.createDataFrame(data, schema=schema)

    out_df = df.withColumn(
        "is_target",
        (col("age") > 30) & (col("score") < 100),
    ).select("id", "age", "score", "is_target").orderBy("id")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "with_logical_column",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "is_target",
                "expr": "col('age') > 30 AND col('score') < 100",
            },
            {"op": "select", "columns": ["id", "age", "score", "is_target"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_filter_nested(spark: SparkSession) -> Dict[str, Any]:
    """Filter with nested logical expressions using parentheses."""
    from pyspark.sql.types import StructType, StructField, IntegerType
    from pyspark.sql.functions import col

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("score", IntegerType(), True),
            StructField("vip", IntegerType(), True),
        ]
    )
    data = [
        (1, 25, 50, 0),
        (2, 35, 60, 0),
        (3, 45, 110, 1),
        (4, 55, 70, 0),
        (5, 30, 80, 1),
    ]
    df = spark.createDataFrame(data, schema=schema)

    # Test: (age > 30 AND (score < 100 OR vip == 1))
    out_df = df.filter(
        (col("age") > 30) & ((col("score") < 100) | (col("vip") == 1))
    ).orderBy("id")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "filter_nested",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "filter",
                "expr": "(col('age') > 30 AND (col('score') < 100 OR col('vip') == 1))",
            },
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def case_with_arithmetic_logical_mix(spark: SparkSession) -> Dict[str, Any]:
    """withColumn combining arithmetic and logical conditions."""
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
    from pyspark.sql.functions import col

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("int_val", IntegerType(), True),
            StructField("double_val", DoubleType(), True),
            StructField("threshold", DoubleType(), True),
        ]
    )
    data = [
        (1, 10, 2.5, 5.0),
        (2, 20, 3.5, 10.0),
        (3, 30, 4.5, 15.0),
        (4, 15, 1.5, 8.0),
    ]
    df = spark.createDataFrame(data, schema=schema)

    # Test: withColumn producing a boolean from arithmetic + logical
    # (int_val + double_val) > threshold
    out_df = df.withColumn(
        "above_threshold",
        (col("int_val") + col("double_val")) > col("threshold"),
    ).select("id", "int_val", "double_val", "threshold", "above_threshold").orderBy("id")

    input_schema = schema_to_json(df.schema)
    input_rows = df_to_rows(df)

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = df_to_rows(out_df)

    return {
        "name": "with_arithmetic_logical_mix",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {
                "op": "withColumn",
                "column": "above_threshold",
                "expr": "(col('int_val') + col('double_val')) > col('threshold')",
            },
            {"op": "select", "columns": ["id", "int_val", "double_val", "threshold", "above_threshold"]},
            {"op": "orderBy", "columns": ["id"], "ascending": [True]},
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
        case_filter_and_or(spark),
        case_filter_not(spark),
        case_filter_nested(spark),
        case_read_csv(spark),
        case_read_parquet(spark),
        case_read_json(spark),
        case_when_otherwise(spark),
        case_when_then_otherwise(spark),
        case_coalesce(spark),
        case_groupby_sum(spark),
        case_groupby_avg(spark),
        case_groupby_min(spark),
        case_groupby_max(spark),
        case_groupby_null_keys(spark),
        case_null_comparison_equality(spark),
        case_null_comparison_ordering(spark),
        case_null_safe_equality(spark),
        case_null_in_filter(spark),
        case_type_coercion_numeric(spark),
        case_type_coercion_mixed(spark),
        case_with_logical_column(spark),
        case_with_arithmetic_logical_mix(spark),
    ]

    for fx in fixtures:
        path = out_dir / f"{fx['name']}.json"
        path.write_text(json.dumps(fx, indent=2))
        print(f"wrote {path}")

    spark.stop()


if __name__ == "__main__":
    main()

