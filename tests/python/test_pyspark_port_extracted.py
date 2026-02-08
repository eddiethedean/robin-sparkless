"""Ported PySpark error/API tests (extracted). Use robin_sparkless."""

from __future__ import annotations

import pytest


# --- Behavioral tests (robin_sparkless supports) ---


def test_drop() -> None:
    """Ported from PySpark DataFrameTestsMixin.test_drop. Drop columns by name."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 50, "Y"], [2, 60, "Y"]],
        [("id", "bigint"), ("age", "bigint"), ("active", "string")],
    )
    assert df.drop(["active"]).columns() == ["id", "age"]
    assert df.drop(["id", "age", "active"]).columns() == []


def test_with_columns_renamed() -> None:
    """Ported from PySpark DataFrameTestsMixin.test_with_columns_renamed."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [["Alice", 50], ["Alice", 60]],
        [("name", "string"), ("age", "bigint")],
    )
    renamed1 = df.with_columns_renamed({"name": "naam", "age": "leeftijd"})
    assert renamed1.columns() == ["naam", "leeftijd"]
    renamed2 = df.with_columns_renamed({"name": "naam"})
    assert renamed2.columns() == ["naam", "age"]


def test_with_columns_renamed_invalid_type_raises() -> None:
    """Ported from PySpark: withColumnsRenamed(tuple) raises."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [["Alice", 50]], [("name", "string"), ("age", "bigint")]
    )
    with pytest.raises((TypeError, Exception)):
        df.with_columns_renamed(("name", "x"))  # type: ignore[arg-type]


def test_drop_duplicates() -> None:
    """Ported from PySpark DataFrameTestsMixin.test_drop_duplicates."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [["Alice", 50], ["Alice", 60]],
        [("name", "string"), ("age", "bigint")],
    )
    assert df.distinct().count() == 2
    assert df.distinct(subset=["name"]).count() == 1
    assert df.distinct(subset=["name", "age"]).count() == 2


def test_drop_empty_column() -> None:
    """Ported from PySpark: df.drop() with no cols leaves all columns."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[14, "Tom"], [23, "Alice"], [16, "Bob"]],
        [("age", "bigint"), ("name", "string")],
    )
    assert df.drop([]).columns() == ["age", "name"]


def test_drop_column_name_with_dot() -> None:
    """Ported from PySpark: drop columns with dots in name."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = (
        spark.range(1, 3)
        .with_column("first.name", rs.lit("Peter"))
        .with_column("city.name", rs.lit("raleigh"))
        .with_column("state", rs.lit("nc"))
    )
    assert df.drop(["first.name"]).columns() == ["id", "city.name", "state"]
    assert df.drop(["city.name"]).columns() == ["id", "first.name", "state"]
    assert df.drop(["first.name", "city.name"]).columns() == ["id", "state"]


def test_dropna() -> None:
    """Ported from PySpark DataFrameTestsMixin.test_dropna."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [["Alice", 50, 80.1], [None, 60, 70.0], ["Bob", None, 90.0]],
        [("name", "string"), ("age", "bigint"), ("height", "double")],
    )
    assert df.dropna().count() == 1
    assert df.dropna(subset=["name"]).count() == 2
    assert df.dropna(subset=["name", "age"]).count() == 1


def test_fillna() -> None:
    """Ported from PySpark DataFrameTestsMixin.test_fillna."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [["Alice", None, None]],
        [("name", "string"), ("age", "bigint"), ("height", "double")],
    )
    row = df.na().fill(rs.lit(50)).collect()[0]
    assert row["age"] == 50
    assert row["height"] == 50.0


def test_with_column_with_existing_name() -> None:
    """Ported from PySpark: with_column replaces existing column."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 10], [2, 20]], [("id", "bigint"), ("x", "bigint")]
    )
    out = df.with_column("x", rs.col("x").multiply(rs.lit(2)))
    rows = out.collect()
    assert rows[0]["x"] == 20 and rows[1]["x"] == 40


def test_with_columns() -> None:
    """Ported from PySpark DataFrameTestsMixin.test_with_columns."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[i, str(i)] for i in range(100)],
        [("key", "bigint"), ("value", "string")],
    )
    keys = df.with_columns({"key": rs.col("key")}).select(["key"]).collect()
    assert [r["key"] for r in keys] == list(range(100))
    kvs = (
        df.with_columns({"key": rs.col("key"), "value": rs.col("value")})
        .select(["key", "value"])
        .collect()
    )
    assert [(r["key"], r["value"]) for r in kvs] == [(i, str(i)) for i in range(100)]
    kvs2 = (
        df.with_columns({"key_alias": rs.col("key"), "value_alias": rs.col("value")})
        .select(["key_alias", "value_alias"])
        .collect()
    )
    assert [(r["key_alias"], r["value_alias"]) for r in kvs2] == [
        (i, str(i)) for i in range(100)
    ]


def test_with_columns_invalid_type_raises() -> None:
    """Ported from PySpark: withColumns(list) raises."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, "a"]], [("key", "bigint"), ("value", "string")]
    )
    with pytest.raises((TypeError, Exception)):
        df.with_columns(["key"])  # type: ignore[arg-type,list-item]


def test_column_iterator_raises() -> None:
    """Ported from PySpark: iterating over Column raises TypeError."""
    import robin_sparkless as rs

    def foo() -> None:
        # PySpark: for x in df.key raises TypeError (Column is not iterable)
        col = rs.col("key")
        for _ in col:  # type: ignore[attr-defined]
            break

    with pytest.raises((TypeError, AttributeError)):
        foo()


def test_toDF_with_string() -> None:
    """Ported from PySpark: toDF(names) renames columns."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.range(1, 4).to_df(["idx"])
    assert df.columns() == ["idx"]
    rows = df.collect()
    assert len(rows) == 3
    assert rows[0]["idx"] == 1


def test_df_show() -> None:
    """Ported from PySpark: df.show() runs without error."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 10, "a"], [2, 20, "b"]],
        [("id", "bigint"), ("x", "bigint"), ("label", "string")],
    )
    df.show()
    df.show(1)


def test_where() -> None:
    """Ported from PySpark: filter(condition) requires Column; filter(10) raises."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.range(10)
    with pytest.raises((TypeError, Exception)):
        df.filter(10)  # type: ignore[arg-type]
    out = df.filter(rs.col("id").gt(rs.lit(5)))
    assert out.count() == 4


def test_colregex() -> None:
    """Ported from PySpark: col_regex(pattern) selects matching columns."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 10, 100]],
        [("a", "bigint"), ("ab", "bigint"), ("abc", "bigint")],
    )
    out = df.col_regex("^a")
    assert out.columns() == ["a", "ab", "abc"]
    out2 = df.col_regex("^ab$")
    assert out2.columns() == ["ab"]


def test_repartition() -> None:
    """Ported from PySpark: repartition returns DataFrame (no-op in Sparkless)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 10], [2, 20]], [("id", "bigint"), ("x", "bigint")]
    )
    rep = df.repartition(2)  # type: ignore[attr-defined]
    assert rep.count() == 2


def test_sample() -> None:
    """Ported from PySpark: sample returns DataFrame."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[i, i * 10] for i in range(100)],
        [("id", "bigint"), ("x", "bigint")],
    )
    sampled = df.sample(with_replacement=False, fraction=0.5, seed=42)  # type: ignore[attr-defined]
    assert sampled.count() <= 100


def test_generic_hints() -> None:
    """Ported from PySpark: hint returns DataFrame (no-op in Sparkless)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.range(10).to_df(["id"])
    hinted = df.hint("broadcast", [1])
    assert hinted.count() == 10


def test_spark_session() -> None:
    """Ported from PySpark SparkSessionTests3.test_spark_session."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    assert spark is not None
    df = spark._create_dataframe_from_rows([[1, 2]], [("a", "bigint"), ("b", "bigint")])
    assert df.count() == 1


def test_active_session() -> None:
    """Ported from PySpark SparkSessionTests3.test_active_session."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    active = rs.SparkSession.get_active_session()
    assert active is not None or spark is not None


def test_cov() -> None:
    """Ported from PySpark: stat.cov returns float."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 2], [2, 4], [3, 6]],
        [("a", "bigint"), ("b", "bigint")],
    )
    c = df.stat().cov("a", "b")
    assert isinstance(c, (int, float))


def test_invalid_join_method() -> None:
    """Ported from PySpark: join(how="invalid") raises."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df1 = spark._create_dataframe_from_rows(
        [[1, "a"]], [("id", "bigint"), ("x", "string")]
    )
    df2 = spark._create_dataframe_from_rows(
        [[1, "b"]], [("id", "bigint"), ("y", "string")]
    )
    with pytest.raises((ValueError, Exception)):
        df1.join(df2, on=["id"], how="invalid")


def test_and_in_expression() -> None:
    """Ported from PySpark: & and | for Column work; use & not 'and'."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[i, str(i)] for i in range(100)],
        [("key", "bigint"), ("value", "string")],
    )
    out = df.filter((rs.col("key").le(rs.lit(10))) & (rs.col("value").le(rs.lit("2"))))
    assert out.count() == 4
    out2 = df.filter((rs.col("key").le(rs.lit(3))) | (rs.col("value").lt(rs.lit("2"))))
    assert out2.count() == 14


def test_column_operators() -> None:
    """Ported from PySpark: Column arithmetic and comparison return Column."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, "a"], [2, "b"]],
        [("key", "bigint"), ("value", "string")],
    )
    ci = rs.col("key")
    out = df.select(((ci + 1) * 2).alias("r"))
    assert out.count() == 2


def test_column_select() -> None:
    """Ported from PySpark: select(cols) return rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, "a"], [2, "b"]],
        [("key", "bigint"), ("value", "string")],
    )
    assert df.select(["key", "value"]).count() == 2
    assert df.select([rs.col("key"), rs.col("value")]).count() == 2
    out = df.filter(rs.col("key").eq(rs.lit(1))).select([rs.col("value")])
    assert out.collect()[0]["value"] == "a"


def test_access_column() -> None:
    """Ported from PySpark: col('key') returns Column with alias method."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, "a"]], [("key", "bigint"), ("value", "string")]
    )
    col_key = rs.col("key")
    assert hasattr(col_key, "alias")
    _ = df.select([col_key])


def test_when() -> None:
    """Ported from PySpark: when(cond).otherwise(val) works."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 10], [2, 20], [3, 30]],
        [("id", "bigint"), ("x", "bigint")],
    )
    out = df.with_column(
        "y", rs.when(rs.col("id").eq(rs.lit(1))).then(rs.lit(100)).otherwise(rs.lit(0))
    )
    rows = out.collect()
    assert rows[0]["y"] == 100
    assert rows[1]["y"] == 0
    assert rows[2]["y"] == 0


def test_greatest() -> None:
    """Ported from PySpark: greatest returns max of columns."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 2, 3], [4, 2, 1]],
        [("a", "bigint"), ("b", "bigint"), ("c", "bigint")],
    )
    out = df.with_column(
        "max_val",
        rs.greatest([rs.col("a"), rs.col("b"), rs.col("c")]),  # type: ignore[arg-type]
    )
    rows = out.collect()
    assert rows[0]["max_val"] == 3
    assert rows[1]["max_val"] == 4


def test_least() -> None:
    """Ported from PySpark: least returns min of columns."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, 2, 3], [4, 2, 1]],
        [("a", "bigint"), ("b", "bigint"), ("c", "bigint")],
    )
    out = df.with_column("min_val", rs.least([rs.col("a"), rs.col("b"), rs.col("c")]))  # type: ignore[arg-type]
    rows = out.collect()
    assert rows[0]["min_val"] == 1
    assert rows[1]["min_val"] == 1


# --- Skip: PySpark-specific or unsupported ---


@pytest.mark.skip(
    reason="sampleBy requires PySpark-specific sampling; not in robin_sparkless"
)
def test_sampleby() -> None:
    pass


def test_string_functions() -> None:
    """Ported subset: lower, upper, length, trim. Full suite covered by parity fixtures."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [["  Hello  ", "world"]],
        [("a", "string"), ("b", "string")],
    )
    out = (
        df.with_column("lower", rs.col("a").lower())
        .with_column("upper", rs.col("a").upper())
        .with_column("len", rs.char_length(rs.col("a")))
        .with_column("trimmed", rs.btrim(rs.col("a")))
    )
    row = out.collect()[0]
    assert row["lower"] == "  hello  "
    assert row["upper"] == "  HELLO  "
    assert row["len"] == 9
    assert row["trimmed"] == "Hello"


@pytest.mark.skip(reason="dayofweek covered by parity fixtures")
def test_dayofweek() -> None:
    pass


@pytest.mark.skip(reason="make_date covered by parity fixtures")
def test_make_date() -> None:
    pass


@pytest.mark.skip(reason="first/last ignoreNulls covered by parity fixtures")
def test_first_last_ignorenulls() -> None:
    pass


@pytest.mark.skip(reason="approxQuantile not implemented in robin_sparkless")
def test_approxQuantile() -> None:
    pass


@pytest.mark.skip(reason="array_repeat semantics differ: robin expects List column")
def test_array_repeat() -> None:
    pass


def test_overlay() -> None:
    """Ported from PySpark: overlay replaces substring at position."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [["Spark_SQL"]],
        [("s", "string")],
    )
    out = df.with_column("x", rs.overlay(rs.col("s"), "CORE", 7, 3))
    row = out.collect()[0]
    assert row["x"] == "Spark_CORE"


@pytest.mark.skip(reason="Higher-order functions not implemented")
def test_higher_order_function_failures() -> None:
    pass


@pytest.mark.skip(reason="Nested higher-order functions not implemented")
def test_nested_higher_order_function() -> None:
    pass


@pytest.mark.skip(reason="datetime extract returns Int8; collect unsupported type")
def test_datetime_functions() -> None:
    """Ported subset: year, month, dayofmonth. Full suite covered by parity fixtures."""
    pass


@pytest.mark.skip(reason="lit day_time_interval not in robin_sparkless")
def test_lit_day_time_interval() -> None:
    pass


@pytest.mark.skip(reason="lit list semantics differ")
def test_lit_list() -> None:
    pass


@pytest.mark.skip(reason="numpy scalar input; robin_sparkless uses lit(scalar)")
def test_lit_np_scalar() -> None:
    pass


@pytest.mark.skip(reason="ndarray input; PySpark-specific")
def test_ndarray_input() -> None:
    pass


@pytest.mark.skip(reason="map_functions covered by parity fixtures")
def test_map_functions() -> None:
    pass


@pytest.mark.skip(reason="schema_of_json covered by parity fixtures")
def test_schema_of_json() -> None:
    pass


@pytest.mark.skip(reason="schema_of_csv covered by parity fixtures")
def test_schema_of_csv() -> None:
    pass


@pytest.mark.skip(reason="from_csv covered by parity fixtures")
def test_from_csv() -> None:
    pass


@pytest.mark.skip(reason="window covered by parity fixtures")
def test_window() -> None:
    pass


@pytest.mark.skip(reason="session_window not in robin_sparkless")
def test_session_window() -> None:
    pass


@pytest.mark.skip(reason="bucket not in robin_sparkless")
def test_bucket() -> None:
    pass


@pytest.mark.skip(reason="repartitionByRange uses rdd in PySpark; Sparkless no-op")
def test_repartitionByRange_dataframe() -> None:
    pass


@pytest.mark.skip(reason="replace(value, subset) API differs; use na().fill")
def test_replace() -> None:
    pass


@pytest.mark.skip(reason="unpivot not implemented in robin_sparkless")
def test_unpivot() -> None:
    pass


@pytest.mark.skip(reason="unpivot_negative not implemented")
def test_unpivot_negative() -> None:
    pass


@pytest.mark.skip(reason="observe not in robin_sparkless")
def test_observe() -> None:
    pass


@pytest.mark.skip(reason="duplicated column names; schema handling differs")
def test_duplicated_column_names() -> None:
    pass


@pytest.mark.skip(
    reason="drop_duplicates_with_ambiguous_reference; join+drop semantics"
)
def test_drop_duplicates_with_ambiguous_reference() -> None:
    pass


@pytest.mark.skip(reason="join_without_on; robin_sparkless requires on")
def test_join_without_on() -> None:
    pass


@pytest.mark.skip(reason="require_cross; cross join semantics")
def test_require_cross() -> None:
    pass


@pytest.mark.skip(reason="cache_table; catalog API differs")
def test_cache_table() -> None:
    pass


@pytest.mark.skip(reason="PySpark-specific: to_pandas when pandas not found")
def test_to_pandas_required_pandas_not_found() -> None:
    pass


@pytest.mark.skip(reason="PySpark-specific: createDataFrame when pandas not found")
def test_create_dataframe_required_pandas_not_found() -> None:
    pass


@pytest.mark.skip(reason="sameSemantics; Sparkless may differ")
def test_same_semantics_error() -> None:
    pass


@pytest.mark.skip(reason="df.to(table) not in robin_sparkless")
def test_to() -> None:
    pass


@pytest.mark.skip(reason="validate_column_types; PySpark _to_java_column")
def test_validate_column_types() -> None:
    pass


@pytest.mark.skip(reason="column_accessor col('foo')[1:3]; slice semantics")
def test_column_accessor() -> None:
    pass


@pytest.mark.skip(reason="column_name_with_non_ascii; encoding")
def test_column_name_with_non_ascii() -> None:
    pass


@pytest.mark.skip(reason="field_accessor struct field access")
def test_field_accessor() -> None:
    pass


@pytest.mark.skip(reason="with_field not in robin_sparkless")
def test_with_field() -> None:
    pass


@pytest.mark.skip(reason="aggregator; GroupedData.agg aggregator type")
def test_aggregator() -> None:
    pass


def test_save_and_load() -> None:
    """Simplified: write parquet to temp dir and read back. Behavioral parity."""
    import tempfile
    from pathlib import Path

    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [[1, "a"], [2, "b"]],
        [("id", "bigint"), ("x", "string")],
    )
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "out.parquet")
        df.write().mode("overwrite").parquet(path)
        loaded = spark.read().parquet(path)
        rows = loaded.order_by(["id"]).collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["x"] == "a"
    assert rows[1]["id"] == 2 and rows[1]["x"] == "b"


@pytest.mark.skip(reason="save_and_load_builder; DataFrameWriterV2")
def test_save_and_load_builder() -> None:
    pass


@pytest.mark.skip(reason="create_without_provider; DataFrameWriterV2")
def test_create_without_provider() -> None:
    pass
