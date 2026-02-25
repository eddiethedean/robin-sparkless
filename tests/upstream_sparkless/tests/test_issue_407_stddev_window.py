"""
Tests for issue #407: stddev() over Window returns None in Sparkless.

PySpark returns the correct sample standard deviation per partition; Sparkless
previously returned None because the Polars window handler did not implement
STDDEV/STDDEV_SAMP. Fixed by adding these in window_handler.py.
"""


class TestIssue407StddevWindow:
    """Test stddev() over Window (issue #407)."""

    def test_stddev_over_window_partition(self, spark):
        """Exact scenario from issue #407: F.stddev('Value').over(w) with partitionBy ID."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame(
            [
                {"ID": 1, "Value": 0},
                {"ID": 1, "Value": 10},
                {"ID": 1, "Value": 5},
                {"ID": 1, "Value": 2},
            ]
        )
        w = Window.partitionBy(F.col("ID"))
        df = df.withColumn("stddev", F.stddev("Value").over(w))
        rows = df.collect()
        assert len(rows) == 4
        stddevs = [r["stddev"] for r in rows]
        # All rows in same partition -> same sample std (PySpark: ~4.349...)
        assert all(s is not None for s in stddevs), "stddev must not be None"
        assert len(set(stddevs)) == 1
        assert 4.0 <= stddevs[0] <= 5.0

    def test_stddev_over_window_multiple_partitions(self, spark):
        """stddev over window with multiple partitions; each partition has same stddev value."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame(
            [
                {"g": "a", "x": 1},
                {"g": "a", "x": 3},
                {"g": "b", "x": 10},
                {"g": "b", "x": 20},
            ]
        )
        w = Window.partitionBy("g")
        df = df.withColumn("stddev", F.stddev("x").over(w))
        rows = df.collect()
        assert len(rows) == 4
        by_g = {}
        for r in rows:
            g, s = r["g"], r["stddev"]
            assert s is not None
            by_g.setdefault(g, []).append(s)
        assert len(by_g["a"]) == 2 and len(by_g["b"]) == 2
        assert by_g["a"][0] == by_g["a"][1]  # same partition -> same stddev
        assert by_g["b"][0] == by_g["b"][1]
        # a: sample std([1,3]) = sqrt(2) ~ 1.41; b: sample std([10,20]) = 7.07...
        assert 1.0 <= by_g["a"][0] <= 2.0
        assert 6.0 <= by_g["b"][0] <= 8.0

    def test_stddev_samp_over_window(self, spark):
        """stddev_samp() over window (alias for stddev) returns same as stddev."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame(
            [{"k": 1, "v": 2}, {"k": 1, "v": 4}, {"k": 1, "v": 6}]
        )
        w = Window.partitionBy("k")
        df = df.withColumn("s", F.stddev_samp("v").over(w))
        rows = df.collect()
        assert len(rows) == 3
        vals = [r["s"] for r in rows]
        assert all(x is not None for x in vals)
        assert len(set(vals)) == 1
        # sample std([2,4,6]) = 2.0
        assert 1.9 <= vals[0] <= 2.1

    def test_stddev_pop_over_window(self, spark):
        """stddev_pop() over window returns population standard deviation."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame(
            [{"g": "a", "x": 2}, {"g": "a", "x": 4}, {"g": "b", "x": 10}]
        )
        w = Window.partitionBy("g")
        df = df.withColumn("pop", F.stddev_pop("x").over(w))
        rows = df.collect()
        assert len(rows) == 3
        by_g = {}
        for r in rows:
            by_g.setdefault(r["g"], []).append(r["pop"])
        # a: population std([2,4]) = 1.0; b: single value -> null or 0.0 (backend-dependent)
        assert len(by_g["a"]) == 2 and by_g["a"][0] == by_g["a"][1]
        assert 0.99 <= by_g["a"][0] <= 1.01
        assert by_g["b"][0] is None or by_g["b"][0] == 0.0

    def test_stddev_over_window_with_nulls(self, spark):
        """stddev ignores nulls in the column; partition stddev is computed on non-null values."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame(
            [
                {"g": "a", "x": 1},
                {"g": "a", "x": None},
                {"g": "a", "x": 3},
            ]
        )
        w = Window.partitionBy("g")
        df = df.withColumn("s", F.stddev("x").over(w))
        rows = df.collect()
        assert len(rows) == 3
        # Sample std of [1, 3] = sqrt(2) ~ 1.414
        for r in rows:
            assert r["s"] is not None
            assert 1.3 <= r["s"] <= 1.5

    def test_stddev_single_row_per_partition_returns_none(self, spark):
        """Sample stddev of one value per partition is None (n-1=0)."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame([{"g": "a", "x": 5}, {"g": "b", "x": 10}])
        w = Window.partitionBy("g")
        df = df.withColumn("s", F.stddev("x").over(w))
        rows = df.collect()
        assert len(rows) == 2
        for r in rows:
            assert r["s"] is None

    def test_stddev_over_window_then_select_and_show(self, spark):
        """withColumn(stddev over window) then select and show works."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame(
            [{"id": 1, "val": 1.0}, {"id": 1, "val": 2.0}, {"id": 2, "val": 3.0}]
        )
        w = Window.partitionBy("id")
        df = df.withColumn("stddev", F.stddev("val").over(w)).select(
            "id", "val", "stddev"
        )
        df.show()
        rows = df.collect()
        assert len(rows) == 3
        for r in rows:
            if r["id"] == 1:
                assert r["stddev"] is not None  # two values
            else:
                assert r["stddev"] is None  # one value
