"""Robust tests for fixes #376–#382.

These tests use the unified `spark` fixture and run with either PySpark or
Sparkless (mock). PySpark results are the baseline; Sparkless should match.

Run with PySpark first to establish baseline:
  MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/test_issues_376_382_robust.py -v

Then run with Sparkless to verify parity:
  MOCK_SPARK_TEST_BACKEND=mock pytest tests/test_issues_376_382_robust.py -v

All 8 tests pass with PySpark. With Sparkless, some tests may fail until
remaining executor/join/select resolution gaps are addressed (self-join,
multi-JOIN, compound join condition, select t1.id resolution).
"""


def _get_functions(spark):
    """Return functions module for the active backend (PySpark or Sparkless)."""
    if "pyspark" in type(spark).__module__:
        from pyspark.sql import functions as F

        return F
    import sparkless.sql.functions as F

    return F


def _val(row, *keys):
    """Get value from row by trying multiple column names (prefixed or not)."""
    for k in keys:
        try:
            if hasattr(row, "asDict"):
                d = row.asDict()
                if k in d:
                    return d[k]
            if hasattr(row, "__getitem__"):
                return row[k]
        except (KeyError, TypeError, AttributeError):
            continue
    return None


def _columns_set(df):
    """Return set of column names (works for both backends)."""
    return set(df.columns)


# -----------------------------------------------------------------------------
# #378 – F.round() on string column with whitespace
# -----------------------------------------------------------------------------


def test_robust_round_string_with_whitespace(spark):
    """#378: F.round() on string column with leading/trailing whitespace."""
    F = _get_functions(spark)

    # Use 10.6 and 20.7 to avoid banker's rounding ambiguity (10.5 -> 10 in Python)
    df = spark.createDataFrame(
        [("  10.6  ",), ("\t20.7\n",)],
        ["val"],
    )
    df = df.withColumn("rounded", F.round("val"))
    rows = df.collect()
    assert len(rows) == 2
    # PySpark strips and casts; Sparkless should match (10.6->11, 20.7->21)
    assert rows[0]["rounded"] == 11.0
    assert rows[1]["rounded"] == 21.0


def test_robust_round_string_with_decimals_and_whitespace(spark):
    """#378: F.round(string_col, 2) with whitespace."""
    F = _get_functions(spark)

    df = spark.createDataFrame([("  3.14159  ",), (" 2.71828 ",)], ["val"])
    df = df.withColumn("r", F.round("val", 2))
    rows = df.collect()
    assert rows[0]["r"] == 3.14
    assert rows[1]["r"] == 2.72


# -----------------------------------------------------------------------------
# #379 – SELECT table-prefixed column (e.g. t1.id) after join
# -----------------------------------------------------------------------------


def test_robust_select_table_prefixed_after_join(spark):
    """#379: df.select('t1.id', 't2.name') after join with aliases t1, t2."""
    F = _get_functions(spark)

    left = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).alias("t1")
    right = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "label"]).alias("t2")
    joined = left.join(right, F.col("t1.id") == F.col("t2.id"), "inner")
    # Select using table-prefixed names; backend may expose t1_id, t2_label or id, label
    result = joined.select("t1.id", "t2.label")
    rows = result.collect()
    assert len(rows) == 2
    cols = _columns_set(result)
    id_col = next((c for c in ("id", "t1_id", "t1.id") if c in cols), None)
    label_col = next((c for c in ("label", "t2_label", "t2.label") if c in cols), None)
    assert id_col is not None
    assert label_col is not None
    # Content: (1,x) and (2,y)
    ids = [_val(r, "id", "t1_id", "t1.id") for r in rows]
    labels = [_val(r, "label", "t2_label", "t2.label") for r in rows]
    assert sorted(ids) == [1, 2]
    assert set(labels) == {"x", "y"}


# -----------------------------------------------------------------------------
# #382 – Self-join with aliases: manager column and row count
# -----------------------------------------------------------------------------


def test_robust_self_join_manager_column_and_row_count(spark):
    """#382: Self-join employees e LEFT JOIN employees m; manager column correct."""
    try:
        emp = spark.createDataFrame(
            [(1, "Alice", None), (2, "Bob", 1), (3, "Carol", 1)],
            ["id", "name", "manager_id"],
        )
        emp.write.mode("overwrite").saveAsTable("employees")

        result = spark.sql(
            """
            SELECT e.name AS employee, m.name AS manager
            FROM employees e
            LEFT JOIN employees m ON e.manager_id = m.id
            """
        )
        rows = result.collect()
        assert len(rows) == 3

        def v(r, *keys):
            for k in keys:
                if k in result.columns:
                    return r[k]
                if hasattr(r, "asDict"):
                    d = r.asDict()
                    if k in d:
                        return d[k]
                try:
                    return r[k]
                except (KeyError, TypeError):
                    pass
            return None

        by_employee = {
            v(r, "employee", "e_name", "name"): v(r, "manager", "m_name") for r in rows
        }
        assert by_employee["Alice"] is None
        assert by_employee["Bob"] == "Alice"
        assert by_employee["Carol"] == "Alice"
    finally:
        spark.sql("DROP TABLE IF EXISTS employees")


# -----------------------------------------------------------------------------
# #380 – Join with compound condition (equality + filter)
# -----------------------------------------------------------------------------


def test_robust_join_compound_condition(spark):
    """#380: Join with (a.id == b.id) & (a.amount > 30)."""
    F = _get_functions(spark)

    orders = spark.createDataFrame(
        [(1, 10, 25.0), (2, 10, 35.0), (3, 20, 45.0)],
        ["order_id", "customer_id", "amount"],
    ).alias("o")
    customers = spark.createDataFrame(
        [(10, "C1"), (20, "C2")],
        ["customer_id", "name"],
    ).alias("c")

    # Join on customer_id AND amount > 30
    cond = (F.col("o.customer_id") == F.col("c.customer_id")) & (F.col("o.amount") > 30)
    result = orders.join(customers, cond, "inner").select(
        "o.order_id", "o.amount", "c.name"
    )
    rows = result.collect()
    # Expect (2, 35, C1) and (3, 45, C2) — two rows
    assert len(rows) == 2
    # Backend may expose amount as "amount", "o_amount", or "o.amount"
    amounts = [_val(r, "amount", "o_amount", "o.amount") for r in rows]
    assert 35.0 in amounts
    assert 45.0 in amounts


# -----------------------------------------------------------------------------
# #381 – SQL WHERE with table-prefixed column (e.salary > 55000)
# -----------------------------------------------------------------------------


def test_robust_sql_where_table_prefixed(spark):
    """#381: WHERE e.salary > 55000 after JOIN."""
    try:
        spark.createDataFrame(
            [(1, "Alice", 10, 50000), (2, "Bob", 20, 60000), (3, "Carol", 10, 70000)],
            ["id", "name", "dept_id", "salary"],
        ).write.mode("overwrite").saveAsTable("employees")
        spark.createDataFrame(
            [(10, "Engineering"), (20, "Sales")],
            ["id", "dept_name"],
        ).write.mode("overwrite").saveAsTable("departments")

        result = spark.sql(
            """
            SELECT e.name, e.salary
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            WHERE e.salary > 55000
            """
        )
        rows = result.collect()
        # Alice 50000 excluded; Bob 60000, Carol 70000 included
        assert len(rows) == 2
        names = [_val(r, "name", "e_name") for r in rows]
        assert "Bob" in names
        assert "Carol" in names
        assert "Alice" not in names
    finally:
        spark.sql("DROP TABLE IF EXISTS employees")
        spark.sql("DROP TABLE IF EXISTS departments")


# -----------------------------------------------------------------------------
# #377 – SQL GROUP BY with table-prefixed column (d.dept_name)
# -----------------------------------------------------------------------------


def test_robust_sql_group_by_table_prefixed(spark):
    """#377: GROUP BY d.dept_name after JOIN."""
    try:
        spark.createDataFrame(
            [(1, "Alice", 10), (2, "Bob", 20), (3, "Carol", 10), (4, "Dave", 20)],
            ["id", "name", "dept_id"],
        ).write.mode("overwrite").saveAsTable("employees")
        spark.createDataFrame(
            [(10, "Engineering"), (20, "Sales")],
            ["id", "dept_name"],
        ).write.mode("overwrite").saveAsTable("departments")

        result = spark.sql(
            """
            SELECT d.dept_name, COUNT(*) AS cnt
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            GROUP BY d.dept_name
            """
        )
        rows = result.collect()
        assert len(rows) == 2
        counts = {_val(r, "dept_name", "d_dept_name"): _val(r, "cnt") for r in rows}
        assert counts["Engineering"] == 2
        assert counts["Sales"] == 2
    finally:
        spark.sql("DROP TABLE IF EXISTS employees")
        spark.sql("DROP TABLE IF EXISTS departments")


# -----------------------------------------------------------------------------
# #376 – SQL 3+ JOINs and SELECT from third table (p.project_name)
# -----------------------------------------------------------------------------


def test_robust_sql_three_joins_select_third_table(spark):
    """#376: FROM e JOIN d ON ... JOIN p ON ... SELECT e.name, d.dept_name, p.project_name."""
    try:
        spark.createDataFrame(
            [(1, "Alice", 10, 100), (2, "Bob", 20, 200)],
            ["id", "name", "dept_id", "project_id"],
        ).write.mode("overwrite").saveAsTable("employees")
        spark.createDataFrame(
            [(10, "Engineering"), (20, "Sales")],
            ["id", "dept_name"],
        ).write.mode("overwrite").saveAsTable("departments")
        spark.createDataFrame(
            [(100, "ProjectA"), (200, "ProjectB")],
            ["id", "project_name"],
        ).write.mode("overwrite").saveAsTable("projects")

        result = spark.sql(
            """
            SELECT e.name, d.dept_name, p.project_name
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            JOIN projects p ON e.project_id = p.id
            """
        )
        rows = result.collect()
        assert len(rows) == 2

        # Must have project_name (or p_project_name)
        cols = _columns_set(result)
        assert "project_name" in cols or "p_project_name" in cols

        name_col = "name" if "name" in cols else "e_name"
        dept_col = "dept_name" if "dept_name" in cols else "d_dept_name"
        proj_col = "project_name" if "project_name" in cols else "p_project_name"

        alice = next(r for r in rows if r[name_col] == "Alice")
        assert alice[dept_col] == "Engineering"
        assert alice[proj_col] == "ProjectA"

        bob = next(r for r in rows if r[name_col] == "Bob")
        assert bob[dept_col] == "Sales"
        assert bob[proj_col] == "ProjectB"
    finally:
        spark.sql("DROP TABLE IF EXISTS employees")
        spark.sql("DROP TABLE IF EXISTS departments")
        spark.sql("DROP TABLE IF EXISTS projects")
