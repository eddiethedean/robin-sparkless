#!/usr/bin/env python3
"""
Create GitHub issues for grouped test failures. Each issue includes
reproduction code for sparkless (current) vs PySpark (expected).
Run from repo root: python scripts/create_failure_issues.py
"""

from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
ISSUES_DIR = REPO_ROOT / ".github" / "issue_bodies"
FAILED_TESTS_FILE = REPO_ROOT / "tmp_failed_tests.txt"


def run_gh_create(title: str, body: str) -> bool:
    result = subprocess.run(
        ["gh", "issue", "create", "--title", title, "--body", body],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        return False
    print(result.stdout.strip())
    return True


def main():
    ISSUES_DIR.mkdir(parents=True, exist_ok=True)

    issues = [
        {
            "title": "[Test parity] select('*') and select('*', col) should expand to all columns",
            "body": """**Expected (PySpark):** `df.select("*")` returns all columns; `df.select("*", "a")` returns all columns plus duplicate `a`.

**Sparkless (current):**
```python
# sparkless
import sparkless
spark = sparkless.SparkSession.builder.appName("x").getOrCreate()
df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
df.select("*").collect()  # SparklessError: column '*' not found
```

**PySpark (expected):**
```python
# PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("x").getOrCreate()
df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
df.select("*").collect()  # [Row(a=1, b=2, c=3)]
df.select("*", "a").columns  # ['a', 'b', 'c', 'a']
```

**Affected tests:** `test_issue_404_select_star.py`, `test_issue_202_select_with_list.py` (select star with list).
""",
        },
        {
            "title": "[Test parity] Filter with dropped column should push down (PySpark semantics)",
            "body": """**Expected (PySpark):** `df.select("col1").filter(F.col("col2").isNotNull())` is valid; filter is pushed below the projection.

**Sparkless (current):**
```python
# sparkless
import sparkless as sp
from sparkless.sql import functions as F
spark = sp.SparkSession.builder.appName("x").getOrCreate()
df = spark.createDataFrame([("a", "b")], ["col1", "col2"])
df_dropped = df.select("col1")
df_dropped.filter(F.col("col2").isNotNull()).collect()
# SparklessError: column 'col2' not found
```

**PySpark (expected):**
```python
# PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("x").getOrCreate()
df = spark.createDataFrame([("a", "b")], ["col1", "col2"])
df_dropped = df.select("col1")
result = df_dropped.filter(F.col("col2").isNotNull()).collect()
# Succeeds: [Row(col1='a')], filter pushed below projection
```

**Affected tests:** `test_issue_158_dropped_column_error.py::test_filter_dropped_column_behavior_matches_pyspark`
""",
        },
        {
            "title": "[Test parity] Row() empty kwargs and Row(**dict) initialization",
            "body": """**Expected (PySpark):** `Row()` with no args raises; `Row(**{"a": 1})` works; `Row(a=1)` works.

**Sparkless (current):**
```python
# sparkless
import sparkless
Row = sparkless.sql.types.Row
Row()  # TypeError: Row() requires at least one value or named field
Row(Column1="Value1", Column2=2)  # May not behave like PySpark for createDataFrame
```

**PySpark (expected):**
```python
# PySpark
from pyspark.sql import Row
Row(a=1, b=2)  # Row(a=1, b=2)
Row(**{"a": 1, "b": 2})  # Row(a=1, b=2)
# Empty Row: PySpark allows Row() in some contexts or raises; dict init works with createDataFrame
```

**Affected tests:** `test_issue_215_row_kwargs_init.py::test_row_empty_kwargs`, `test_row_dict_initialization_still_works`
""",
        },
        {
            "title": "[Test parity] try_cast invalid string to date should return null",
            "body": """**Expected (PySpark):** `try_cast("not-a-date", "date")` returns null for invalid values.

**Sparkless (current):**
```python
# sparkless
import sparkless as sp
from sparkless.sql import functions as F
spark = sp.SparkSession.builder.appName("x").getOrCreate()
df = spark.createDataFrame([("not-a-date",)], ["s"])
df.select(F.try_cast(F.col("s"), "date").alias("d")).collect()
# SparklessError: conversion from str to date failed for value "not-a-date"
```

**PySpark (expected):**
```python
# PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("x").getOrCreate()
df = spark.createDataFrame([("not-a-date",)], ["s"])
df.select(F.try_cast(F.col("s"), "date").alias("d")).collect()
# [Row(d=None)]  # try_cast returns null for invalid input
```

**Affected tests:** `test_issue_216_date_cast_datetime_string.py::test_try_cast_datetime_string_to_date_invalid_null`
""",
        },
        {
            "title": "[Test parity] String + string with + operator (PySpark yields null for non-numeric)",
            "body": """**Expected (PySpark):** For string columns, `col + col` or `col + lit("x")` treats + as numeric; non-numeric strings yield null.

**Sparkless (current):** May return concatenated string or different semantics.

**PySpark (expected):**
```python
# PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("x").getOrCreate()
df = spark.createDataFrame([("hello", "world")], ["a", "b"])
df.select((F.col("a") + F.col("b")).alias("c")).collect()
# [Row(c=None)]  # + on strings is numeric in PySpark, so null
```

**Affected tests:** `test_issue_188_string_concat_cache.py` (multiple tests)
""",
        },
        {
            "title": "[Test parity] asc_nulls_last and orderBy null ordering",
            "body": """**Expected (PySpark):** `functions.asc_nulls_last()`, `desc_nulls_first()` exist for null ordering in orderBy.

**Sparkless (current):**
```python
# sparkless
from sparkless.sql import functions as F
F.asc_nulls_last  # AttributeError: no attribute 'asc_nulls_last'
```

**PySpark (expected):**
```python
# PySpark
from pyspark.sql import functions as F
F.asc_nulls_last(F.col("x"))  # Column with nulls last ascending
```

**Affected tests:** `test_issue_327_orderby_ascending.py::test_orderby_with_null_values`, `test_orderby_mixed_nulls_and_values`
""",
        },
        {
            "title": "[Test parity] create_map: map value type when collected (string vs int)",
            "body": """**Expected (PySpark):** When collecting, create_map column values from DataFrame columns may be stringified in Row/dict; test expects `'1'` for integer column.

**Sparkless (current):**
```python
# sparkless
df.select(F.create_map(F.lit("key1"), F.col("val1"), F.lit("key2"), F.col("val2")).alias("map_col")).collect()
# Row(map_col={'key1': 'a', 'key2': 1})  # sparkless keeps int
```

**PySpark (expected):**
```python
# PySpark - test expects map value as string for col value
# Row(map_col={'key1': 'a', 'key2': '1'})
```

**Affected tests:** `test_create_map.py::test_create_map_with_literals`, `test_create_map_with_null_keys`
""",
        },
        {
            "title": "[Test parity] SparkContext.appName (camelCase) for session.sparkContext",
            "body": """**Expected (PySpark):** `spark.sparkContext.appName` (property).

**Sparkless (current):**
```python
# sparkless
spark.sparkContext.appName  # AttributeError: 'PySparkContext' has no attribute 'appName'. Did you mean: 'app_name'?
```

**PySpark (expected):**
```python
# PySpark
spark.sparkContext.appName  # 'myApp'
```

**Affected tests:** `test_fixture_compatibility.py::test_sparkcontext_available_in_session`
""",
        },
        {
            "title": "[Test parity] createDataFrame kwargs: verifySchema, samplingRatio",
            "body": """**Expected (PySpark):** `spark.createDataFrame(data, schema, verifySchema=True)`, `createDataFrame(..., samplingRatio=0.01)` accepted.

**Sparkless (current):**
```python
# sparkless
spark.createDataFrame([], schema=..., verifySchema=True)  # TypeError: unexpected keyword argument 'verifySchema'
spark.createDataFrame(..., samplingRatio=None)  # TypeError: unexpected keyword argument 'samplingRatio'
```

**PySpark (expected):** Accepts these optional kwargs.

**Affected tests:** `test_issue_372_create_data_frame.py` (verify_schema, sampling_ratio, empty_data_with_schema, mixed_dict_and_list_rows_raises, etc.)
""",
        },
        {
            "title": "[Test parity] posexplode with two-name alias and posexplode_outer",
            "body": """**Expected (PySpark):** `posexplode(col).alias("pos", "val")` or similar; `posexplode_outer` implemented.

**Sparkless (current):** Length mismatch when aliasing posexplode to two names; `posexplode_outer` raises NotImplementedError.

**Affected tests:** `test_issue_366_alias_posexplode.py` (multiple), `test_issue_429_posexplode_no_alias.py`
""",
        },
        {
            "title": "[Test parity] SQL temp view / table not found under pytest-xdist",
            "body": """**Observed:** Many SQL/unit tests fail with "Table or view 'X' not found" when run with `pytest -n 10`. Views are created in one worker and not visible to the session used in the test (session/worker isolation).

**Affected tests (sample):** parity/sql (employees, order_test, limit_test, ...), unit/session (test_sql_cte_robust, test_sql_in_clause, test_sql_like_clause, test_sql_update), test_column_case_variations (employees), test_issue_362_sql_drop_table, test_issue_395_catalog_set_current_database_read_csv, test_issue_414_conf_is_case_sensitive (if session differs), test_issue_418_nested_ddl.

**Reproduction:** Run `pytest tests/parity/sql tests/unit/session -n 10`; many tests register temp views then run SQL that references them; in another worker the session may not see those views.

**Possible directions:** Ensure tests that use SQL + temp views either use a single session (no xdist for those) or document that SQL + temp view tests should run with `-n 0` or use a shared session fixture.
""",
        },
        {
            "title": "[Test parity] Window first/last value semantics",
            "body": """**Expected (PySpark):** `first(col, ignorenulls).over(w)` / `last(col, ignorenulls).over(w)` respect window ordering and null handling.

**Sparkless (current):** test expects `last_sal` = 90 but gets 100 (ordering or frame semantics differ).

**Affected tests:** `test_window_pyspark_parity.py::test_window_first_last_pyspark_parity`
""",
        },
        {
            "title": "[Test parity] get_json_object and xxhash64 return type / null",
            "body": """**Expected (PySpark):** `get_json_object` returns string (JSON string); `xxhash64` returns long, with null for null input where test expects 42 for non-null.

**Sparkless (current):** get_json_object may return dict; xxhash64 may return None where test expects 42.

**Affected tests:** `test_issue_189_string_functions_robust.py::test_get_json_object_missing_path_and_invalid_json`, `test_xxhash64_known_values_and_null`
""",
        },
        {
            "title": "[Test parity] createDataFrame from RDD not implemented",
            "body": """**Expected (PySpark):** `spark.createDataFrame(rdd, schema)` supported.

**Sparkless (current):** `NotImplementedError: rdd is not yet implemented in robin-sparkless`.

**Affected tests:** `test_issue_361_createDataFrame_rdd.py` (all tests in that file).
""",
        },
        {
            "title": "[Test parity] Join result column names (no _right suffix)",
            "body": """**Expected (PySpark):** When joining on columns with same name, PySpark may not suffix the right side; test expects no `id_right` / `a_right` in row keys.

**Sparkless (current):** Row has extra key `id_right` or `a_right`.

**Affected tests:** `test_issue_353_join_on_accept_column.py::test_join_on_column`, `test_join_on_list_of_columns`
""",
        },
        {
            "title": "[Test parity] Schema inference for numeric columns",
            "body": """**Expected (PySpark):** createDataFrame with numeric-looking data infers LongType/DoubleType.

**Sparkless (current):** Infers StringType for numeric columns.

**Affected tests:** `test_issue_164_schema_inference_numeric.py`
""",
        },
        {
            "title": "[Test parity] Struct field alias and nested struct select",
            "body": """**Expected (PySpark):** Selecting struct field with alias (e.g. `col("struct").getField("f").alias("A")`) exposes the alias in Row; nested struct behavior.

**Sparkless (current):** AssertionError assert 'A' is None (alias not visible or wrong).

**Affected tests:** `test_issue_330_struct_field_alias.py`
""",
        },
        {
            "title": "[Test parity] Pandas createDataFrame column order",
            "body": """**Expected (PySpark):** createDataFrame(pandas_df) preserves column order; show() column order matches.

**Sparkless (current):** Column order differs (e.g. alphabetical or other).

**Affected tests:** `test_issue_372_pandas_column_order.py`, `test_create_dataframe_from_pandas_preserves_column_order`
""",
        },
        {
            "title": "[Test parity] explain(mode=...) and describe extended",
            "body": """**Expected (PySpark):** df.explain(mode="extended") or similar accepted.

**Affected tests:** `test_issue_384_describe_show_explain.py::test_explain_accepts_mode`
""",
        },
        {
            "title": "[Test parity] ArrayType.containsNull / nullable",
            "body": """**Expected (PySpark):** ArrayType has containsNull (or nullable) attribute; test expects False for non-nullable array.

**Sparkless (current):** getattr(ArrayType, 'containsNull', True) is True when test expects False.

**Affected tests:** `test_array_type_robust.py::test_array_type_elementtype_with_non_nullable_array`
""",
        },
        {
            "title": "[Test parity] Hour/minute/second with timezone formats (EST vs UTC)",
            "body": """**Expected (PySpark):** With session timezone set, hour/minute/second from timestamp strings respect timezone (e.g. EST 9 vs UTC 4).

**Sparkless (current):** Returns 4 (UTC) when test expects 9 (EST).

**Affected tests:** `test_issue_294_hour_minute_second_string_timestamps.py::test_hour_minute_second_with_different_timezone_formats`
""",
        },
        {
            "title": "Full list of 213 failing tests (pytest -n 10)",
            "body": None,  # filled below from tmp_failed_tests.txt
        },
    ]

    # Fill "Full list" issue body from tmp_failed_tests.txt
    failed_path = REPO_ROOT / "tmp_failed_tests.txt"
    if failed_path.exists():
        failed_list = failed_path.read_text()
        for i, issue in enumerate(issues):
            if issue.get("body") is None and "Full list" in issue["title"]:
                issues[i]["body"] = (
                    """Consolidated list of all test failures from the latest run with `pytest tests -n 10 -v --tb=short`.

Use this as an index; individual parity issues are filed separately with reproduction code (sparkless vs PySpark).

<details>
<summary>Failed tests (click to expand)</summary>

```
"""
                    + failed_list
                    + """
```
</details>

To regenerate: run pytest with -n 10 and grep '^FAILED ' from output.
"""
                )
                break
    else:
        # Remove the Full list issue if no file
        issues = [i for i in issues if i.get("body") is not None]

    created = 0
    for issue in issues:
        if issue.get("body") is None:
            continue
        if run_gh_create(issue["title"], issue["body"]):
            created += 1
    print(f"\nCreated {created} issues.", file=sys.stderr)
    return 0 if created == len(issues) else 1


if __name__ == "__main__":
    sys.exit(main())
