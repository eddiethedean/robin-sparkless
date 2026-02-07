"""
Smoke tests for the robin_sparkless Python module (PyO3 bridge).

Run after building the extension (requires an activated virtualenv for maturin develop):
  python -m venv .venv && . .venv/bin/activate   # or: source .venv/bin/activate
  pip install maturin pytest
  maturin develop --features pyo3
  pytest tests/python/
  # or: python -m pytest tests/python/
"""

from __future__ import annotations

import pytest


def test_import_module() -> None:
    """Module can be imported."""
    import robin_sparkless

    assert robin_sparkless is not None


def test_configure_for_multiprocessing() -> None:
    """configure_for_multiprocessing() exists and can be called (no-op after first use)."""
    import robin_sparkless as rs

    # Call is idempotent; limits Polars to 1 thread for fork-safety
    rs.configure_for_multiprocessing()


def test_spark_session_builder() -> None:
    """SparkSession.builder().app_name(...).get_or_create() works."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    assert spark is not None
    assert spark.is_case_sensitive() in (True, False)


def test_create_dataframe_and_collect() -> None:
    """create_dataframe with list of 3-tuples and collect returns list of dicts."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Carol")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    assert df is not None
    n = df.count()
    assert n == 3
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert rows[0] == {"id": 1, "age": 25, "name": "Alice"}
    assert rows[1]["name"] == "Bob"
    assert rows[2]["age"] == 35


def test_filter_and_select() -> None:
    """filter(expr) and select(cols) return DataFrame; collect matches."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Carol")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    # filter: age > 28
    filtered = df.filter(rs.col("age").gt(rs.lit(28)))
    assert filtered.count() == 2
    rows = filtered.collect()
    assert all(r["age"] > 28 for r in rows)
    # select columns
    selected = df.select(["id", "name"])
    assert selected.count() == 3
    first = selected.collect()[0]
    assert "id" in first and "name" in first
    assert "age" not in first


def test_filter_with_and_or_operators() -> None:
    """filter with (Column & Column) and (Column | Column) works (fixes #9, #10)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Carol"), (4, 40, "Dave")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    # AND: age > 26 & age < 36
    filtered_and = df.filter(
        (rs.col("age").gt(rs.lit(26))) & (rs.col("age").lt(rs.lit(36)))
    )
    rows_and = filtered_and.collect()
    assert len(rows_and) == 2
    assert all(26 < r["age"] < 36 for r in rows_and)
    # OR: age < 26 | age > 35
    filtered_or = df.filter(
        (rs.col("age").lt(rs.lit(26))) | (rs.col("age").gt(rs.lit(35)))
    )
    rows_or = filtered_or.collect()
    assert len(rows_or) == 2
    assert rows_or[0]["age"] == 25 and rows_or[1]["age"] == 40


def test_with_column_and_show() -> None:
    """with_column adds a column; show runs without error."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 25, "Alice"), (2, 30, "Bob")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    doubled = df.with_column("age2", rs.col("age").gt(rs.lit(20)))
    assert doubled.count() == 2
    doubled.show(5)


def test_group_by_count() -> None:
    """group_by(...).count() returns DataFrame with group keys and count."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 0, "a"), (2, 0, "a"), (3, 0, "b")], ["id", "age", "grp"]
    )
    grouped = df.group_by(["grp"])
    result = grouped.count()
    assert result is not None
    rows = result.collect()
    assert len(rows) == 2
    counts = {r["grp"]: r["count"] for r in rows}
    assert counts["a"] == 2
    assert counts["b"] == 1


def test_col_lit_when() -> None:
    """col, lit, when().then().otherwise() build expressions."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 10, ""), (2, 20, ""), (3, 30, "")], ["id", "age", "name"]
    )
    expr = (
        rs.when(rs.col("age").gt(rs.lit(15)))
        .then(rs.lit("high"))
        .otherwise(rs.lit("low"))
    )
    out = df.with_column("level", expr)
    rows = out.collect()
    assert len(rows) == 3
    assert rows[0]["level"] == "low"
    assert rows[1]["level"] == "high"
    assert rows[2]["level"] == "high"


def test_limit_and_distinct() -> None:
    """limit(n) and distinct() behave correctly."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 1, "a"), (2, 2, "b"), (3, 3, "c")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    limited = df.limit(2)
    assert limited.count() == 2
    distinct_df = spark.create_dataframe(
        [(1, 1, "x"), (1, 1, "x"), (2, 2, "y")], ["id", "age", "name"]
    )
    uniq = distinct_df.distinct()
    assert uniq.count() == 2


def test_aggregate_functions() -> None:
    """Module-level sum, avg, min, max, count work on columns."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "a"), (3, 30, "b")], ["id", "val", "grp"]
    )
    grouped = df.group_by(["grp"])
    agg_df = grouped.agg([rs.sum(rs.col("val")), rs.count(rs.col("id"))])
    rows = agg_df.collect()
    assert len(rows) == 2


def test_stat_cov_corr() -> None:
    """df.stat().cov(col1, col2) and .corr(col1, col2) return float."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "x", "name"]
    )
    stat = df.stat()
    c = stat.cov("id", "x")
    assert isinstance(c, (int, float))
    r = stat.corr("id", "x")
    assert isinstance(r, (int, float))
    assert -1.0 - 1e-9 <= r <= 1.0 + 1e-9 or (r != r)  # NaN


def test_na_fill_drop() -> None:
    """df.na().fill(value) and df.na().drop() exist and run."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "age", "name"]
    )
    filled = df.na().fill(rs.lit(0))
    assert filled.count() == 3
    dropped = df.na().drop()
    assert dropped.count() == 3


def test_with_columns_and_renamed() -> None:
    """with_columns and with_columns_renamed work (dict or list of tuples)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    out = df.with_columns({"extra": rs.lit(42)})
    rows = out.collect()
    assert rows[0]["extra"] == 42
    renamed = df.with_columns_renamed({"name": "full_name"})
    row = renamed.collect()[0]
    assert "full_name" in row and "name" not in row


def test_to_pandas() -> None:
    """to_pandas returns list of dicts (same as collect)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    result = df.to_pandas()
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 1
    assert (
        result[0]["id"] == 1 and result[0]["age"] == 25 and result[0]["name"] == "Alice"
    )


def test_ascii_base64() -> None:
    """ascii(column) and base64(column) exist and run."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 65, "A")], ["id", "code", "name"])
    out = df.with_column("ascii_val", rs.ascii(rs.col("name")))
    assert out.count() == 1
    df2 = spark.create_dataframe([(1, 2, "hello")], ["id", "x", "msg"])
    out2 = df2.with_column("enc", rs.base64(rs.col("msg")))
    assert out2.count() == 1


def test_filter_nonexistent_column_raises() -> None:
    """Filter with non-existent column raises an error."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    with pytest.raises(Exception):
        df.filter(rs.col("nonexistent").gt(rs.lit(0)))


def test_select_nonexistent_column_raises() -> None:
    """Select with non-existent column raises an error."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    with pytest.raises(Exception):
        df.select(["id", "nonexistent"])


def test_delta_write_and_read(spark) -> None:
    """When built with delta feature: write_delta then read_delta round-trips data."""
    import tempfile

    df = spark.create_dataframe([(1, 1, "a"), (2, 2, "b")], ["id", "num", "name"])
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/delta_table"
            df.write_delta(path, overwrite=True)
            back = spark.read_delta(path)
            rows = back.collect()
            assert len(rows) == 2
            assert rows[0]["id"] == 1 and rows[0]["name"] == "a"
    except (RuntimeError, AttributeError) as e:
        if (
            isinstance(e, AttributeError)
            or "delta" in str(e).lower()
            or "Delta Lake" in str(e)
        ):
            pytest.skip(
                "Delta Lake feature not built (build with --features pyo3,delta)"
            )
        raise


# --- Sparkless parity tests (PR-S: issues #1-#21). Expectations match PySpark. ---


def test_sparkless_parity_join_inner_returns_rows() -> None:
    """Join (inner) returns rows. PySpark: inner join on key yields matched rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    left = spark.create_dataframe([(1, 10, "a"), (2, 20, "b")], ["id", "v", "label"])
    right = spark.create_dataframe([(1, 100, "x"), (3, 300, "z")], ["id", "w", "tag"])
    joined = left.join(right, ["id"], "inner")
    rows = joined.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1 and rows[0]["v"] == 10 and rows[0]["w"] == 100


def test_sparkless_parity_join_left_returns_rows() -> None:
    """Join (left) returns rows. PySpark: left join keeps all left rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    left = spark.create_dataframe(
        [(1, 0, "a"), (2, 0, "b")], ["id", "_", "label"]
    ).drop(["_"])
    right = spark.create_dataframe([(1, 0, "x")], ["id", "_", "tag"]).drop(["_"])
    joined = left.join(right, ["id"], "left")
    rows = joined.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["tag"] == "x"
    assert rows[1]["id"] == 2 and rows[1]["tag"] is None


def test_sparkless_parity_join_right_returns_rows() -> None:
    """Join (right) returns rows. PySpark: right join keeps all right rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    left = spark.create_dataframe([(1, 0, "x")], ["id", "_", "tag"]).drop(["_"])
    right = spark.create_dataframe(
        [(1, 0, "a"), (2, 0, "b")], ["id", "_", "label"]
    ).drop(["_"])
    joined = left.join(right, ["id"], "right")
    rows = joined.collect()
    assert len(rows) == 2


def test_sparkless_parity_join_outer_returns_rows() -> None:
    """Join (outer) returns rows. PySpark: full outer keeps all keys from both sides."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    left = spark.create_dataframe(
        [(1, 0, "a"), (2, 0, "b")], ["id", "_", "label"]
    ).drop(["_"])
    right = spark.create_dataframe([(1, 0, "x"), (3, 0, "z")], ["id", "_", "tag"]).drop(
        ["_"]
    )
    joined = left.join(right, ["id"], "outer")
    rows = joined.collect()
    assert len(rows) == 3


def test_sparkless_parity_join_left_semi_returns_rows() -> None:
    """Join (left_semi) returns rows. PySpark: only left columns, rows that have a match in right."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    left = spark.create_dataframe(
        [(1, 0, "a"), (2, 0, "b"), (3, 0, "c")], ["id", "_", "label"]
    ).drop(["_"])
    right = spark.create_dataframe([(2, 0, "x")], ["id", "_", "tag"]).drop(["_"])
    joined = left.join(right, ["id"], "left_semi")
    rows = joined.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 2 and rows[0]["label"] == "b"
    assert "tag" not in rows[0]


def test_sparkless_parity_join_left_anti_returns_rows() -> None:
    """Join (left_anti) returns rows. PySpark: only left columns, rows with no match in right."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    left = spark.create_dataframe(
        [(1, 0, "a"), (2, 0, "b"), (3, 0, "c")], ["id", "_", "label"]
    ).drop(["_"])
    right = spark.create_dataframe([(2, 0, "x")], ["id", "_", "tag"]).drop(["_"])
    joined = left.join(right, ["id"], "left_anti")
    rows = joined.collect()
    assert len(rows) == 2
    ids = {r["id"] for r in rows}
    assert ids == {1, 3}


def test_sparkless_parity_filter_simple_returns_rows() -> None:
    """Filter (simple) returns rows. PySpark: filter(expr) returns matching rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Carol")], ["id", "age", "name"]
    )
    out = df.filter(rs.col("age").gt(rs.lit(28)))
    rows = out.collect()
    assert len(rows) == 2
    assert all(r["age"] > 28 for r in rows)


def test_sparkless_parity_filter_boolean_returns_rows() -> None:
    """Filter (boolean column) returns rows. PySpark: filter(boolean_col) keeps True rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 0, "a"), (2, 0, "b"), (3, 0, "c")], ["id", "_", "name"]
    )
    df = df.with_column(
        "flag",
        rs.when(rs.col("id").eq(rs.lit(2))).then(rs.lit(False)).otherwise(rs.lit(True)),
    ).drop(["_"])
    out = df.filter(rs.col("flag").eq(rs.lit(True)))
    rows = out.collect()
    assert len(rows) == 2
    assert [r["id"] for r in rows] == [1, 3]


def test_sparkless_parity_select_returns_rows() -> None:
    """Select (column access) returns rows. PySpark: select(cols) returns same number of rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 10, "a"), (2, 20, "b")], ["id", "v", "name"])
    out = df.select(["id", "name"])
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["name"] == "a"


def test_sparkless_parity_select_with_alias_returns_rows() -> None:
    """Select with alias returns rows. PySpark: select(col.alias(...)) preserves row count."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    df = df.with_column("years", rs.col("age"))
    out = df.select(["years", "name"])
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["years"] == 25 and rows[0]["name"] == "Alice"


def test_sparkless_parity_with_column_returns_rows() -> None:
    """withColumn returns rows. PySpark: withColumn preserves row count."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 0, "a"), (2, 0, "b")], ["id", "_", "name"]).drop(
        ["_"]
    )
    out = df.with_column("double_id", rs.col("id").multiply(rs.lit(2)))
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["double_id"] == 2 and rows[1]["double_id"] == 4


def test_sparkless_parity_drop_returns_rows() -> None:
    """Drop column returns rows. PySpark: drop(cols) preserves row count."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 10, "a"), (2, 20, "b")], ["id", "v", "name"])
    out = df.drop(["v"])
    rows = out.collect()
    assert len(rows) == 2
    assert "v" not in rows[0] and "id" in rows[0] and "name" in rows[0]


def test_sparkless_parity_distinct_returns_rows() -> None:
    """distinct returns rows. PySpark: distinct() returns one row per unique row."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 0, "a"), (1, 0, "a"), (2, 0, "b")], ["id", "_", "name"]
    ).drop(["_"])
    out = df.distinct()
    rows = out.collect()
    assert len(rows) == 2


def test_sparkless_parity_order_by_desc_returns_rows() -> None:
    """orderBy desc returns rows. PySpark: orderBy(col, ascending=False) preserves rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe_from_rows(
        [[1, "a"], [2, "b"], [3, "c"]], [("id", "bigint"), ("name", "string")]
    )
    out = df.order_by(["id"], ascending=[False])
    rows = out.collect()
    assert len(rows) == 3
    assert rows[0]["id"] == 3 and rows[2]["id"] == 1


def test_sparkless_parity_filter_comparison_not_column_existence() -> None:
    """Filter comparison operations not treated as column existence. PySpark: col('a') > 1 is comparison."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "v", "x"]
    ).drop(["x"])
    out = df.filter(rs.col("v").gt(rs.lit(15)))
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["v"] == 20 and rows[1]["v"] == 30


def test_sparkless_parity_table_read_returns_rows() -> None:
    """Table read (createOrReplaceTempView + table) returns correct row count. PySpark parity."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "v", "name"]
    )
    try:
        spark.create_or_replace_temp_view("t", df)
    except AttributeError:
        pytest.skip("sql feature not built (create_or_replace_temp_view not available)")
    read_back = spark.table("t")
    rows = read_back.collect()
    assert len(rows) == 3
    assert rows[0]["id"] == 1 and rows[2]["name"] == "c"


def test_sparkless_parity_multiple_append_operations() -> None:
    """Multiple append-like operations (union) preserve rows. PySpark: union stacks rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    a = spark.create_dataframe([(1, 0, "a")], ["id", "_", "name"]).drop(["_"])
    b = spark.create_dataframe([(2, 0, "b")], ["id", "_", "name"]).drop(["_"])
    c = spark.create_dataframe([(3, 0, "c")], ["id", "_", "name"]).drop(["_"])
    combined = a.union(b).union(c)
    rows = combined.collect()
    assert len(rows) == 3
    assert rows[0]["id"] == 1 and rows[1]["id"] == 2 and rows[2]["id"] == 3


def test_sql_select_where_returns_rows() -> None:
    """SQL SELECT with WHERE returns filtered rows (#122-#140 session/SQL parity)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "v", "name"]
    )
    try:
        spark.create_or_replace_temp_view("t", df)
        result = spark.sql("SELECT * FROM t WHERE id > 1")
    except AttributeError:
        pytest.skip("sql feature not built")
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 2 and rows[1]["id"] == 3
    assert rows[0]["name"] == "b" and rows[1]["name"] == "c"


# Predetermined expected output for create_dataframe_from_rows (int/string/boolean/date).
# Derived from PySpark 3.5 createDataFrame with schema "id INT, name STRING, ok BOOLEAN, d DATE"
# and rows [(1, "Alice", True, date(2024,1,15)), (2, "Bob", False, date(2024,6,10))].
# Tests run only robin-sparkless and assert against this; no PySpark at test runtime.
EXPECTED_CREATE_DATAFRAME_FROM_ROWS_PARITY = [
    {"id": 1, "name": "Alice", "ok": True, "d": "2024-01-15"},
    {"id": 2, "name": "Bob", "ok": False, "d": "2024-06-10"},
]


def test_create_dataframe_from_rows_schema_pyspark_parity() -> None:
    """create_dataframe_from_rows matches predetermined PySpark expectations (#151)."""
    import robin_sparkless as rs

    schema = [("id", "int"), ("name", "string"), ("ok", "boolean"), ("d", "date")]
    rows = [
        {"id": 1, "name": "Alice", "ok": True, "d": "2024-01-15"},
        {"id": 2, "name": "Bob", "ok": False, "d": "2024-06-10"},
    ]
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe_from_rows(rows, schema)
    result = sorted(df.collect(), key=lambda r: r["id"])
    assert result == EXPECTED_CREATE_DATAFRAME_FROM_ROWS_PARITY


def test_regexp_extract_all_and_select_with_expression() -> None:
    """regexp_extract_all and select with Column expressions (issue #176)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    schema = [("s", "string")]
    df = spark.create_dataframe_from_rows(data, schema)
    # PySpark-style: select with expression (regexp_extract_all returns array of matches)
    result = df.select([rs.regexp_extract_all(rs.col("s"), r"\d+", 0).alias("m")])
    rows = result.collect()
    assert len(rows) == 3
    assert rows[0]["m"] == ["1", "22", "333"]
    assert rows[1]["m"] == []
    assert rows[2]["m"] is None
    # Also support select with varargs: select(expr)
    result2 = df.select(rs.regexp_extract_all(rs.col("s"), r"\d+", 0).alias("m"))
    rows2 = result2.collect()
    assert rows2 == rows
    # Column names still work: select(["s"]) and select("s")
    result3 = df.select(["s"])
    assert result3.collect()[0]["s"] == "a1 b22 c333"
    result4 = df.select("s")
    assert result4.collect()[0]["s"] == "a1 b22 c333"


def test_pivot_raises_not_implemented() -> None:
    """pivot() raises NotImplementedError (#156 stub)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe_from_rows(
        [[1, "x", 10]], [("id", "bigint"), ("pcol", "string"), ("v", "bigint")]
    )
    with pytest.raises(NotImplementedError, match="pivot is not yet implemented"):
        df.pivot("pcol")
