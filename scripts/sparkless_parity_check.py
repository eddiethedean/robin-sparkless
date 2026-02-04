#!/usr/bin/env python3
"""
Run common PySpark-idiomatic code against Sparkless and record failures (parity issues).

Usage:
  pip install sparkless  # if not already
  python scripts/sparkless_parity_check.py

Output: List of (check_name, exception_or_diff) for reporting to Sparkless repo.
Does not require PySpark or robin-sparkless.
"""

from __future__ import annotations

import sys
import warnings
from typing import Any, Callable, List, Optional, Tuple

warnings.filterwarnings("ignore", message=".*LazyFrame.*")
warnings.filterwarnings("ignore", message=".*schema.*", module=".*materializer.*")

Result = Tuple[str, Optional[Exception], Optional[str], Any]
# (check_name, exception, note, result_or_none)


def _run(name: str, fn: Callable[[], Any]) -> Result:
    try:
        out = fn()
        return (name, None, None, out)
    except Exception as e:  # noqa: BLE001
        return (name, e, None, None)


def _rows_eq(a: Any, b: Any) -> bool:
    """Compare collect() results (list of Row or dict)."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if len(a) != len(b):
        return False
    for i, ra in enumerate(a):
        rb = b[i]
        da = ra.asDict() if hasattr(ra, "asDict") else (ra if isinstance(ra, dict) else dict(ra))
        db = rb.asDict() if hasattr(rb, "asDict") else (rb if isinstance(rb, dict) else dict(rb))
        if da != db:
            return False
    return True


def run_sparkless_checks() -> List[Result]:
    from sparkless.sql import SparkSession
    import sparkless.sql.functions as F

    results: List[Result] = []
    spark: Any = None

    def get_spark():
        nonlocal spark
        if spark is None:
            # Use attribute, not call (known workaround for builder())
            spark = SparkSession.builder.appName("parity_check").getOrCreate()
        return spark

    # --- Session / builder ---
    r = _run("SparkSession.builder() callable", lambda: SparkSession.builder().appName("x").getOrCreate())
    results.append(r)

    # --- createDataFrame: list of tuples + column names ---
    def _create_and_collect():
        s = get_spark()
        data = [(1, 25, "alice"), (2, 30, "bob")]
        df = s.createDataFrame(data, ["id", "age", "name"])
        rows = df.collect()
        # Verify column order: first column should be id (value 1, 2)
        assert len(rows) == 2
        r0 = rows[0]
        d = r0.asDict() if hasattr(r0, "asDict") else dict(r0)
        if d.get("id") != 1 and d.get("age") == 1:
            raise AssertionError(f"createDataFrame column order wrong: first col should be id, got {list(d.keys())} -> {d}")
        return rows
    results.append(_run("createDataFrame(tuples, names) column order", _create_and_collect))

    # --- union (position-based) ---
    def _union_position():
        s = get_spark()
        data_a = [(1, 25, "a"), (2, 30, "b")]
        data_b = [(3, 22, "c"), (4, 28, "d")]
        df_a = s.createDataFrame(data_a, ["id", "age", "name"])
        df_b = s.createDataFrame(data_b, ["id", "age", "name"])
        return df_a.union(df_b).collect()
    results.append(_run("union() position-based", _union_position))

    # --- select with single str vs list ---
    def _select_single_str():
        s = get_spark()
        df = s.createDataFrame([(1, "x")], ["a", "b"])
        return df.select("a").collect()
    results.append(_run("select single column as str", _select_single_str))

    # --- filter with column expr ---
    def _filter_col():
        s = get_spark()
        df = s.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])
        return df.filter(F.col("v") >= 20).collect()
    results.append(_run("filter(F.col(...) >= literal)", _filter_col))

    # --- withColumn ---
    def _with_column():
        s = get_spark()
        df = s.createDataFrame([(1, 2)], ["a", "b"])
        return df.withColumn("c", F.col("a") + F.col("b")).collect()
    results.append(_run("withColumn expr", _with_column))

    # --- empty DataFrame count ---
    def _empty_count():
        s = get_spark()
        df = s.createDataFrame([], "a: int, b: string")
        return df.count()
    results.append(_run("createDataFrame([], schema_str) and count()", _empty_count))

    # --- limit(0) ---
    def _limit_zero():
        s = get_spark()
        df = s.createDataFrame([(1,), (2,)], ["x"])
        return df.limit(0).collect()
    results.append(_run("limit(0)", _limit_zero))

    # --- .schema and .columns ---
    def _schema_columns():
        s = get_spark()
        df = s.createDataFrame([(1, "y")], ["id", "name"])
        schema = df.schema
        cols = df.columns
        if cols != ["id", "name"]:
            raise AssertionError(f"df.columns order: expected ['id','name'], got {cols}")
        return (str(schema), cols)
    results.append(_run("schema / columns order", _schema_columns))

    # --- createOrReplaceTempView + sql ---
    def _temp_view_sql():
        s = get_spark()
        df = s.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])
        df.createOrReplaceTempView("t")
        out = s.sql("SELECT id, label FROM t WHERE id = 2").collect()
        return out
    results.append(_run("createOrReplaceTempView + sql", _temp_view_sql))

    # --- join with on as list ---
    def _join_on_list():
        s = get_spark()
        df1 = s.createDataFrame([(1, "x")], ["id", "a"])
        df2 = s.createDataFrame([(1, "y")], ["id", "b"])
        return df1.join(df2, ["id"], "inner").collect()
    results.append(_run("join(..., on=['id'], how='inner')", _join_on_list))

    # --- dropDuplicates() no args ---
    def _drop_duplicates():
        s = get_spark()
        df = s.createDataFrame([(1, 1), (1, 2), (2, 1)], ["a", "b"])
        return df.dropDuplicates().collect()
    results.append(_run("dropDuplicates() no args", _drop_duplicates))

    # --- orderBy(F.desc(...)) ---
    def _order_by_desc():
        s = get_spark()
        df = s.createDataFrame([(1, 10), (2, 30), (3, 20)], ["id", "v"])
        return df.orderBy(F.desc("v")).collect()
    results.append(_run("orderBy(F.desc('col'))", _order_by_desc))

    # --- first() on non-empty ---
    def _first():
        s = get_spark()
        df = s.createDataFrame([(1, "a")], ["id", "n"])
        return df.first()
    results.append(_run("first()", _first))

    # --- toPandas ---
    def _to_pandas():
        s = get_spark()
        df = s.createDataFrame([(1, "x")], ["a", "b"])
        return df.toPandas()
    results.append(_run("toPandas()", _to_pandas))

    # --- fillna ---
    def _fillna():
        s = get_spark()
        df = s.createDataFrame([(1, None), (2, "y")], ["id", "name"])
        return df.fillna("default").collect()
    results.append(_run("fillna(value)", _fillna))

    # --- replace ---
    def _replace():
        s = get_spark()
        df = s.createDataFrame([(1, "a"), (2, "b"), (3, "a")], ["id", "c"])
        return df.replace("a", "A").collect()
    results.append(_run("replace(old, new)", _replace))

    # --- distinct() ---
    def _distinct():
        s = get_spark()
        df = s.createDataFrame([(1,), (1,), (2,)], ["x"])
        return df.distinct().collect()
    results.append(_run("distinct()", _distinct))

    # --- createDataFrame from list of dicts ---
    def _create_from_dicts():
        s = get_spark()
        data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        df = s.createDataFrame(data)
        return df.collect()
    results.append(_run("createDataFrame(list of dicts)", _create_from_dicts))

    # --- groupBy().agg(F.sum(...)) ---
    def _groupby_agg():
        s = get_spark()
        df = s.createDataFrame([(1, 10), (1, 20), (2, 5)], ["k", "v"])
        return df.groupBy("k").agg(F.sum("v").alias("total")).collect()
    results.append(_run("groupBy().agg(F.sum().alias())", _groupby_agg))

    # --- Window + row_number ---
    def _window_row_number():
        from sparkless.sql import Window
        s = get_spark()
        df = s.createDataFrame([("a", 1), ("a", 2), ("b", 1)], ["g", "v"])
        w = Window.partitionBy("g").orderBy("v")
        return df.withColumn("rn", F.row_number().over(w)).collect()
    results.append(_run("Window.partitionBy().orderBy() + row_number()", _window_row_number))

    # --- F.when / F.otherwise ---
    def _when_otherwise():
        s = get_spark()
        df = s.createDataFrame([(1,), (2,), (3,)], ["x"])
        return df.withColumn("label", F.when(F.col("x") < 2, "low").otherwise("high")).collect()
    results.append(_run("F.when().otherwise()", _when_otherwise))

    # --- coalesce (repartition) ---
    def _coalesce():
        s = get_spark()
        df = s.createDataFrame([(1,), (2,)], ["x"])
        return df.coalesce(1).count()
    results.append(_run("coalesce(1)", _coalesce))

    # --- df.write.format().mode().save (optional; may need path) ---
    def _write_parquet():
        import tempfile
        import os
        s = get_spark()
        df = s.createDataFrame([(1, "a")], ["id", "n"])
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "out")
            df.write.format("parquet").mode("overwrite").save(path)
        return "ok"
    results.append(_run("write.format('parquet').mode('overwrite').save(path)", _write_parquet))

    # --- StructType schema in createDataFrame ---
    def _create_struct_schema():
        try:
            from sparkless.sql import types as sparkless_types
            StructType = sparkless_types.StructType
            StructField = sparkless_types.StructField
            IntegerType = sparkless_types.IntegerType
            StringType = sparkless_types.StringType
        except (ImportError, AttributeError):
            from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        s = get_spark()
        schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        df = s.createDataFrame([(1, "x"), (2, "y")], schema)
        return df.collect()
    results.append(_run("createDataFrame with StructType schema", _create_struct_schema))

    # --- F.col with cast ---
    def _col_cast():
        s = get_spark()
        df = s.createDataFrame([(1, "2"), (3, "4")], ["a", "b"])
        return df.select(F.col("a"), F.col("b").cast("int")).collect()
    results.append(_run("F.col().cast('int')", _col_cast))

    # --- describe / summary ---
    def _summary():
        s = get_spark()
        df = s.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        return df.summary().collect()
    results.append(_run("summary()", _summary))

    # --- subtract (except) ---
    def _subtract():
        s = get_spark()
        a = s.createDataFrame([(1,), (2,), (3,)], ["x"])
        b = s.createDataFrame([(2,)], ["x"])
        return a.subtract(b).collect()
    results.append(_run("subtract(other)", _subtract))

    # --- intersect ---
    def _intersect():
        s = get_spark()
        a = s.createDataFrame([(1,), (2,)], ["x"])
        b = s.createDataFrame([(2,), (3,)], ["x"])
        return a.intersect(b).collect()
    results.append(_run("intersect(other)", _intersect))

    # --- crossJoin ---
    def _cross_join():
        s = get_spark()
        a = s.createDataFrame([(1,)], ["a"])
        b = s.createDataFrame([(10,), (20,)], ["b"])
        return a.crossJoin(b).collect()
    results.append(_run("crossJoin(other)", _cross_join))

    # --- withColumnRenamed ---
    def _with_column_renamed():
        s = get_spark()
        df = s.createDataFrame([(1, "x")], ["a", "b"])
        return df.withColumnRenamed("a", "id").columns
    results.append(_run("withColumnRenamed(old, new)", _with_column_renamed))

    # --- drop with list ---
    def _drop_columns():
        s = get_spark()
        df = s.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        return df.drop("b").columns
    results.append(_run("drop('col')", _drop_columns))

    # --- countDistinct ---
    def _count_distinct():
        s = get_spark()
        df = s.createDataFrame([(1,), (1,), (2,)], ["x"])
        return df.agg(F.countDistinct("x")).collect()
    results.append(_run("agg(F.countDistinct('col'))", _count_distinct))

    # --- alias in select ---
    def _select_alias():
        s = get_spark()
        df = s.createDataFrame([(1, 10)], ["a", "b"])
        return df.select(F.col("a").alias("id"), (F.col("b") * 2).alias("double_b")).collect()
    results.append(_run("select with alias", _select_alias))

    # --- orderBy with list of columns ---
    def _order_by_list():
        s = get_spark()
        df = s.createDataFrame([(1, 2, 3), (1, 1, 4), (2, 0, 5)], ["a", "b", "c"])
        return df.orderBy(["a", "b"]).collect()
    results.append(_run("orderBy([list of columns])", _order_by_list))

    # --- na.drop ---
    def _na_drop():
        s = get_spark()
        df = s.createDataFrame([(1, None), (2, "y")], ["id", "name"])
        return df.na.drop().collect()
    results.append(_run("na.drop()", _na_drop))

    # --- F.struct ---
    def _struct():
        s = get_spark()
        df = s.createDataFrame([(1, "a", 10)], ["id", "n", "v"])
        return df.select(F.struct(F.col("id"), F.col("n")).alias("s")).collect()
    results.append(_run("F.struct(...)", _struct))

    # --- F.array ---
    def _array():
        s = get_spark()
        df = s.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        return df.select(F.array(F.col("a"), F.col("b")).alias("arr")).collect()
    results.append(_run("F.array(...)", _array))

    return results


def main() -> int:
    try:
        from sparkless.sql import SparkSession  # noqa: F401
    except ImportError:
        print("sparkless not installed. pip install sparkless", file=sys.stderr)
        return 1

    results = run_sparkless_checks()
    failed = [(n, e) for n, e, _, _ in results if e is not None]
    passed = [n for n, e, _, _ in results if e is None]

    print("--- Sparkless parity checks ---")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")
    if failed:
        print("\nFailed checks (parity issues):")
        for name, err in failed:
            print(f"  [{name}]")
            print(f"    {type(err).__name__}: {err}")
    if passed:
        print("\nPassed:", ", ".join(passed[:15]) + (" ..." if len(passed) > 15 else ""))

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
