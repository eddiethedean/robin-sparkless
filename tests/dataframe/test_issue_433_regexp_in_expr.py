"""Test issue #433: REGEXP and RLIKE in F.expr().

PySpark supports df.filter(F.expr("Value REGEXP 'sales|tech'")) and RLIKE.
Sparkless SQLExprParser previously raised ParseException for these expressions.
Now parses REGEXP and RLIKE in F.expr() for PySpark parity.

https://github.com/eddiethedean/sparkless/issues/433
"""

from sparkless.testing import get_imports


def test_filter_regexp_exact_issue_433(spark, spark_mode):
    """Exact scenario from issue #433 - Value REGEXP 'sales|tech'."""
    F_backend = get_imports(spark_mode).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales department"},
            {"Name": "Bob", "Value": "tech department"},
            {"Name": "Charlie", "Value": "ceo"},
        ]
    )
    df = df.filter(F_backend.expr("Value REGEXP 'sales|tech'"))
    df.show()
    rows = df.collect()
    assert len(rows) == 2
    names = {r["Name"] for r in rows}
    assert names == {"Alice", "Bob"}


def test_filter_rlike_same_as_regexp(spark, spark_mode):
    """RLIKE is alias for REGEXP in F.expr()."""
    F_backend = get_imports(spark_mode).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales"},
            {"Name": "Bob", "Value": "tech"},
            {"Name": "Charlie", "Value": "ceo"},
        ]
    )
    df = df.filter(F_backend.expr("Value RLIKE 'sales|tech'"))
    rows = df.collect()
    assert len(rows) == 2
    names = {r["Name"] for r in rows}
    assert names == {"Alice", "Bob"}


def test_expr_regexp_with_column(spark, spark_mode):
    """F.expr with REGEXP used in withColumn."""
    F_backend = get_imports(spark_mode).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales dept"},
            {"Name": "Bob", "Value": "tech dept"},
            {"Name": "Charlie", "Value": "ceo"},
        ]
    )
    df = df.withColumn("matches", F_backend.expr("Value REGEXP 'sales|tech'"))
    rows = df.collect()
    alice = next(r for r in rows if r["Name"] == "Alice")
    bob = next(r for r in rows if r["Name"] == "Bob")
    charlie = next(r for r in rows if r["Name"] == "Charlie")
    assert alice["matches"] is True
    assert bob["matches"] is True
    assert charlie["matches"] is False


def test_expr_regexp_single_match(spark, spark_mode):
    """REGEXP with single pattern match."""
    F_backend = get_imports(spark_mode).F
    df = spark.createDataFrame([{"s": "hello world"}, {"s": "goodbye"}])
    df = df.filter(F_backend.expr("s REGEXP 'hello'"))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "hello world"


def test_expr_regexp_no_match(spark, spark_mode):
    """REGEXP returns empty when no rows match."""
    F_backend = get_imports(spark_mode).F
    df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
    df = df.filter(F_backend.expr("Name REGEXP 'xyz'"))
    rows = df.collect()
    assert len(rows) == 0
