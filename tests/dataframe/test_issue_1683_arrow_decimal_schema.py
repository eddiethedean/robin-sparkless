"""Regression coverage for preserving PyArrow decimal schemas (#1683)."""

from __future__ import annotations

from decimal import Decimal

import pytest


def test_pyarrow_decimal_schema_precision_values_and_nulls(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    arrow_schema = pa.schema([("amount", pa.decimal128(12, 2))])
    table = pa.Table.from_pylist(
        [{"amount": Decimal("10.50")}, {"amount": None}],
        schema=arrow_schema,
    )

    df = spark.createDataFrame(table)

    assert df.schema["amount"].dataType.simpleString() == "decimal(12,2)"
    assert "amount: decimal(12,2)" in df.printSchema()
    rows = df.collect()
    assert rows[0]["amount"] == Decimal("10.50")
    assert rows[1]["amount"] is None


def test_pyarrow_decimal_values_with_exponent_notation(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.Table.from_pylist(
        [{"amount": Decimal("1E-7")}, {"amount": Decimal("1E+3")}],
        schema=pa.schema([("amount", pa.decimal128(12, 7))]),
    )

    rows = spark.createDataFrame(table).collect()

    assert [row["amount"] for row in rows] == [
        Decimal("0.0000001"),
        Decimal("1000.0000000"),
    ]


def test_python_decimal_inference_and_decimal_string_schema(spark) -> None:
    df = spark.createDataFrame(
        [{"amount": Decimal("0.10")}, {"amount": Decimal("10.50")}]
    )

    assert df.schema["amount"].dataType.simpleString() == "decimal(4,2)"
    assert [row["amount"] for row in df.collect()] == [
        Decimal("0.10"),
        Decimal("10.50"),
    ]

    as_string = spark.createDataFrame(
        [{"amount": Decimal("1.50")}], schema=[("amount", "string")]
    )
    assert as_string.collect()[0]["amount"] == "1.50"


def test_decimal_overflow_raises_instead_of_becoming_null(spark) -> None:
    with pytest.raises(Exception, match="decimal|precision|overflow"):
        spark.createDataFrame(
            [{"amount": Decimal("1E+39")}], schema="amount decimal(38,0)"
        )


def test_to_arrow_preserves_decimal_schema(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    df = spark.createDataFrame(
        [{"amount": Decimal("10.50")}], schema="amount decimal(12,2)"
    )

    table = df.toArrow()

    assert table.schema.field("amount").type == pa.decimal128(12, 2)
    assert table.to_pylist() == [{"amount": Decimal("10.50")}]


def test_decimal_filter_and_aggregation_remain_exact(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    from sparkless.testing import get_imports

    table = pa.Table.from_pylist(
        [
            {"kind": "g", "amount": Decimal("10.50")},
            {"kind": "g", "amount": Decimal("2.25")},
            {"kind": "g", "amount": Decimal("0.10")},
        ],
        schema=pa.schema(
            [
                ("kind", pa.string()),
                ("amount", pa.decimal128(12, 2)),
            ]
        ),
    )
    F = get_imports().F

    row = (
        spark.createDataFrame(table)
        .filter("amount >= 2.00")
        .groupBy("kind")
        .agg(F.sum("amount").alias("total"))
        .collect()[0]
    )

    assert row["total"] == Decimal("12.75")


def test_decimal_filter_keeps_large_integral_values_exact(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.Table.from_pylist(
        [
            {"amount": Decimal("9007199254740992")},
            {"amount": Decimal("9007199254740993")},
        ],
        schema=pa.schema([("amount", pa.decimal128(20, 0))]),
    )

    rows = spark.createDataFrame(table).filter("amount = 9007199254740993").collect()

    assert [row["amount"] for row in rows] == [Decimal("9007199254740993")]


def test_decimal_filter_keeps_literal_scale(spark) -> None:
    df = spark.createDataFrame(
        [{"amount": Decimal("2.00")}], schema="amount decimal(12,2)"
    )

    assert len(df.filter("amount < 2.001").collect()) == 1
    assert df.filter("amount = 2.001").collect() == []


def test_decimal_filter_literal_wider_than_column_precision(spark) -> None:
    df = spark.createDataFrame(
        [{"amount": Decimal("2.00")}], schema="amount decimal(3,2)"
    )

    assert len(df.filter("amount < 10").collect()) == 1
    assert len(df.filter("amount < 10.001").collect()) == 1


def test_decimal_sum_and_casts_keep_decimal_values(spark) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    df = spark.createDataFrame(
        [{"amount": Decimal("0.10")}, {"amount": Decimal("0.20")}],
        schema="amount decimal(12,2)",
    )

    total = df.agg(F.sum("amount").alias("total")).collect()[0]["total"]
    assert total == Decimal("0.30")
    average_df = df.agg(F.avg("amount").alias("average"))
    assert average_df.schema["average"].dataType.simpleString() == "decimal(16,6)"
    assert average_df.collect()[0]["average"] == Decimal("0.150000")
    assert df.select(df.amount.cast("double").alias("amount")).collect() == [
        {"amount": 0.1},
        {"amount": 0.2},
    ]
    cast_back = df.select(df.amount.cast("decimal(12,2)").alias("amount"))
    assert cast_back.schema["amount"].dataType.simpleString() == "decimal(12,2)"
    assert [row["amount"] for row in cast_back.collect()] == [
        Decimal("0.10"),
        Decimal("0.20"),
    ]


def test_decimal_cast_rescales_with_spark_half_up_rounding(spark) -> None:
    df = spark.createDataFrame(
        [{"amount": Decimal("1.25")}, {"amount": Decimal("-1.25")}],
        schema="amount decimal(12,2)",
    )

    rounded = df.select(df.amount.cast("decimal(12,1)").alias("amount")).collect()

    assert [row["amount"] for row in rounded] == [Decimal("1.3"), Decimal("-1.3")]


def test_decimal_average_keeps_large_values_and_rounds_exactly(spark) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    large = spark.createDataFrame(
        [
            {"amount": Decimal("9007199254740993.01")},
            {"amount": Decimal("9007199254740993.03")},
        ],
        schema="amount decimal(20,2)",
    )
    repeating = spark.createDataFrame(
        [
            {"amount": Decimal("1.00")},
            {"amount": Decimal("1.00")},
            {"amount": Decimal("0.00")},
        ],
        schema="amount decimal(12,2)",
    )
    grouped = spark.createDataFrame(
        [
            {"kind": "first", "amount": Decimal("1.00")},
            {"kind": "first", "amount": Decimal("1.00")},
            {"kind": "first", "amount": Decimal("0.00")},
            {"kind": "second", "amount": Decimal("9007199254740993.01")},
            {"kind": "second", "amount": Decimal("9007199254740993.03")},
        ],
        schema="kind string, amount decimal(20,2)",
    )

    assert large.agg(F.avg("amount").alias("average")).collect()[0][
        "average"
    ] == Decimal("9007199254740993.020000")
    assert repeating.agg(F.avg("amount").alias("average")).collect()[0][
        "average"
    ] == Decimal("0.666667")
    grouped_averages = {
        row["kind"]: row["average"]
        for row in grouped.groupBy("kind")
        .agg(F.avg("amount").alias("average"))
        .collect()
    }
    assert grouped_averages == {
        "first": Decimal("0.666667"),
        "second": Decimal("9007199254740993.020000"),
    }


def test_decimal_sum_widens_precision_for_results(spark) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    df = spark.createDataFrame(
        [
            {"amount": Decimal("9999999999.99")},
            {"amount": Decimal("9999999999.99")},
        ],
        schema="amount decimal(12,2)",
    )

    total = df.agg(F.sum("amount").alias("total"))

    assert total.schema["total"].dataType.simpleString() == "decimal(22,2)"
    assert total.collect()[0]["total"] == Decimal("19999999999.98")


def test_decimal_join_and_union_do_not_collapse_adjacent_large_values(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    left = spark.createDataFrame(
        pa.Table.from_pylist(
            [{"id": Decimal("9007199254740992")}],
            schema=pa.schema([("id", pa.decimal128(20, 0))]),
        )
    )
    right = spark.createDataFrame(
        pa.Table.from_pylist(
            [{"id": Decimal("9007199254740993")}],
            schema=pa.schema([("id", pa.decimal128(21, 0))]),
        )
    )

    assert left.join(right, "id").collect() == []
    unioned = left.unionByName(right).collect()
    assert [row["id"] for row in unioned] == [
        Decimal("9007199254740992"),
        Decimal("9007199254740993"),
    ]


def test_decimal38_comparison_does_not_overflow_valid_values(spark) -> None:
    largest = Decimal("9" * 38)
    df = spark.createDataFrame([{"amount": largest}], schema="amount decimal(38,0)")

    assert [row["amount"] for row in df.filter("amount > 0.1").collect()] == [largest]


def test_wide_decimal_comparison_keeps_adjacent_scales_exact(spark) -> None:
    df = spark.createDataFrame(
        [
            {
                "whole": Decimal("10000000000000000"),
                "fractional": Decimal("10000000000000000.1"),
            }
        ],
        schema="whole decimal(38,0), fractional decimal(38,1)",
    )

    assert len(df.filter("whole < fractional").collect()) == 1
    assert df.filter("whole = fractional").collect() == []
    assert df.filter("whole > fractional").collect() == []


def test_decimal_map_values_are_decimal_after_collection(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    from sparkless.testing import get_imports

    arrow_map_type = pa.map_(pa.string(), pa.decimal128(12, 2))
    table = pa.Table.from_pylist(
        [{"balances": [("checking", Decimal("10.25"))]}],
        schema=pa.schema([("balances", arrow_map_type)]),
    )

    df = spark.createDataFrame(table)
    row = df.collect()[0]

    assert isinstance(df.schema["balances"].dataType, get_imports().MapType)
    assert row["balances"] == {"checking": Decimal("10.25")}
    exported = df.toArrow()
    assert exported.schema.field("balances").type == arrow_map_type
    assert exported.to_pylist() == [{"balances": [("checking", Decimal("10.25"))]}]


def test_arrow_nested_decimal_and_int32_types_survive_import(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    schema = pa.schema(
        [
            ("details", pa.struct([("amount", pa.decimal128(12, 2))])),
            ("ids", pa.list_(pa.int32())),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "details": {"amount": Decimal("10.25")},
                "ids": [1, 2],
            }
        ],
        schema=schema,
    )

    row = spark.createDataFrame(table).collect()[0]

    assert row["details"]["amount"] == Decimal("10.25")
    assert row["ids"] == [1, 2]


def test_arrow_uint64_values_remain_exact_after_decimal_import(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    values = [2**63 + 1, 2**64 - 1]
    table = pa.table({"value": pa.array(values, type=pa.uint64())})

    df = spark.createDataFrame(table)

    assert df.schema["value"].dataType.simpleString() == "decimal(20,0)"
    expected = [Decimal(str(value)) for value in values]
    assert [row["value"] for row in df.collect()] == expected
    exported = df.toArrow()
    assert exported.schema.field("value").type == pa.decimal128(20, 0)
    assert exported.to_pylist() == [{"value": value} for value in expected]


def test_arrow_int16_and_smallint_schemas_use_short_type(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    values = [-32768, 32767]
    table = pa.table({"value": pa.array(values, type=pa.int16())})

    df = spark.createDataFrame(table)

    assert df.schema["value"].dataType.simpleString() == "smallint"
    assert [row["value"] for row in df.collect()] == values
    assert df.toArrow().schema.field("value").type == pa.int16()

    explicitly_typed = spark.createDataFrame(
        [{"value": 32767}], schema="value smallint"
    )
    assert explicitly_typed.schema["value"].dataType.simpleString() == "smallint"
    assert explicitly_typed.collect()[0]["value"] == 32767


def test_union_smallint_and_integer_uses_integer_type(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    short_df = spark.createDataFrame(
        pa.table({"value": pa.array([1], type=pa.int16())})
    )
    int_df = spark.createDataFrame(pa.table({"value": pa.array([2], type=pa.int32())}))

    result = short_df.unionByName(int_df)

    assert result.schema["value"].dataType.simpleString() == "int"
    assert [row["value"] for row in result.collect()] == [1, 2]


@pytest.mark.parametrize("value", ["not-an-int", 2**31])
def test_int32_schema_verification_rejects_invalid_values(spark, value) -> None:
    with pytest.raises(TypeError, match="Row 0: column 'value' expected type int32"):
        spark.createDataFrame([{"value": value}], schema="value int32")


def test_negative_decimal_inference_keeps_precision_within_38(spark) -> None:
    value = Decimal("-" + "9" * 38)

    df = spark.createDataFrame([{"amount": value}])

    assert df.schema["amount"].dataType.simpleString() == "decimal(38,0)"
    assert df.collect()[0]["amount"] == value


def test_arrow_empty_and_nested_decimal_types_survive_round_trip(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("amounts", pa.list_(pa.decimal128(12, 2))),
        ]
    )
    empty = spark.createDataFrame(pa.Table.from_pylist([], schema=schema))

    empty_arrow = empty.toArrow()
    assert empty_arrow.schema == schema

    populated = spark.createDataFrame(
        pa.Table.from_pylist([{"id": 1, "amounts": [Decimal("10.25")]}], schema=schema)
    )
    round_trip = populated.toArrow()
    assert round_trip.schema.field("amounts").type == pa.list_(pa.decimal128(12, 2))
    assert round_trip.to_pylist() == [{"id": 1, "amounts": [Decimal("10.25")]}]


def test_arrow_decimal_schema_survives_other_arrow_types(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.Table.from_pylist(
        [{"amount": Decimal("10.50"), "items": [1, 2]}],
        schema=pa.schema(
            [
                ("amount", pa.decimal128(12, 2)),
                ("items", pa.list_(pa.int64())),
            ]
        ),
    )

    df = spark.createDataFrame(table)

    assert df.schema["amount"].dataType.simpleString() == "decimal(12,2)"
    assert df.collect()[0]["items"] == [1, 2]


def test_decimal_arrow_export_uses_the_transformed_column_type(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    df = spark.createDataFrame(
        pa.Table.from_pylist(
            [{"amount": Decimal("10.50")}],
            schema=pa.schema([("amount", pa.decimal128(12, 2))]),
        )
    )

    as_float = df.select(df.amount.cast("double").alias("amount")).toArrow()
    assert as_float.schema.field("amount").type == pa.float64()
    assert as_float.to_pylist() == [{"amount": 10.5}]

    with_new_scale = df.select(
        df.amount.cast("decimal(12,3)").alias("amount")
    ).toArrow()
    assert with_new_scale.schema.field("amount").type == pa.decimal128(12, 3)
    assert with_new_scale.to_pylist() == [{"amount": Decimal("10.500")}]


def test_arrow_array_of_key_value_structs_stays_an_array_after_projection(
    spark,
) -> None:
    pa = pytest.importorskip("pyarrow")
    schema = pa.schema(
        [
            (
                "entries",
                pa.list_(
                    pa.struct(
                        [
                            ("key", pa.string()),
                            ("value", pa.decimal128(12, 2)),
                        ]
                    )
                ),
            )
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "entries": [
                    {"key": "same", "value": Decimal("1.25")},
                    {"key": "same", "value": Decimal("2.50")},
                ]
            }
        ],
        schema=schema,
    )
    df = spark.createDataFrame(table)
    expected = [
        {"key": "same", "value": Decimal("1.25")},
        {"key": "same", "value": Decimal("2.50")},
    ]

    projected = df.select(df.entries.alias("renamed"))
    assert projected.collect()[0]["renamed"] == expected
    assert (
        df.withColumnRenamed("entries", "renamed").collect()[0]["renamed"] == expected
    )
    assert df.toPandas().iloc[0]["entries"] == expected


def test_decimal_inference_does_not_silently_reduce_scale(spark) -> None:
    with pytest.raises(Exception, match="decimal|precision|scale"):
        spark.createDataFrame([{"amount": Decimal("1E-39")}])


def test_decimal_inference_counts_digits_before_positive_exponent(spark) -> None:
    value = Decimal("9.99E+37")

    df = spark.createDataFrame([{"amount": value}])

    assert df.schema["amount"].dataType.simpleString() == "decimal(38,0)"
    assert df.collect()[0]["amount"] == value


def test_ordered_decimal_windows_keep_running_sum_and_average_exact(spark) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    Window = get_imports().Window
    df = spark.createDataFrame(
        [
            {"seq": 2, "amount": Decimal("2.00")},
            {"seq": 1, "amount": Decimal("9007199254740993.01")},
            {"seq": 3, "amount": None},
        ],
        schema="seq int, amount decimal(20,2)",
    )
    window = Window.orderBy("seq").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    result = df.withColumn("running_sum", F.sum("amount").over(window)).withColumn(
        "running_avg", F.avg("amount").over(window)
    )
    rows = sorted(result.collect(), key=lambda row: row["seq"])

    assert result.schema["running_sum"].dataType.simpleString() == "decimal(30,2)"
    assert result.schema["running_avg"].dataType.simpleString() == "decimal(24,6)"
    assert [row["running_sum"] for row in rows] == [
        Decimal("9007199254740993.01"),
        Decimal("9007199254740995.01"),
        Decimal("9007199254740995.01"),
    ]
    assert [row["running_avg"] for row in rows] == [
        Decimal("9007199254740993.010000"),
        Decimal("4503599627370497.505000"),
        Decimal("4503599627370497.505000"),
    ]

    range_window = Window.orderBy("seq").rangeBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    range_result = df.withColumn(
        "running_sum", F.sum("amount").over(range_window)
    ).withColumn("running_avg", F.avg("amount").over(range_window))
    range_rows = sorted(range_result.collect(), key=lambda row: row["seq"])
    assert [row["running_sum"] for row in range_rows] == [
        Decimal("9007199254740993.01"),
        Decimal("9007199254740995.01"),
        Decimal("9007199254740995.01"),
    ]
    assert [row["running_avg"] for row in range_rows] == [
        Decimal("9007199254740993.010000"),
        Decimal("4503599627370497.505000"),
        Decimal("4503599627370497.505000"),
    ]


def test_ordered_integer_window_sum_keeps_bits_above_float_precision(spark) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    Window = get_imports().Window
    df = spark.createDataFrame(
        [(1, 9_007_199_254_740_992), (2, 1)],
        schema="seq int, amount bigint",
    )
    window = Window.orderBy("seq").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    result = df.withColumn("running_sum", F.sum("amount").over(window))
    rows = sorted(result.collect(), key=lambda row: row["seq"])

    assert result.schema["running_sum"].dataType.simpleString() == "bigint"
    assert [row["running_sum"] for row in rows] == [
        9_007_199_254_740_992,
        9_007_199_254_740_993,
    ]


@pytest.mark.parametrize("frame_kind", ["rows", "range"])
def test_decimal_window_average_overflow_obeys_ansi_mode(spark, frame_kind) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    Window = get_imports().Window
    df = spark.createDataFrame([(1, Decimal("1E+34"))], "seq int, amount decimal(38,0)")
    window = Window.orderBy("seq")
    if frame_kind == "rows":
        window = window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    else:
        window = window.rangeBetween(Window.unboundedPreceding, Window.currentRow)

    previous_ansi = spark.conf.get("spark.sql.ansi.enabled", "true")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        result = df.withColumn("average", F.avg("amount").over(window))
        assert result.collect()[0]["average"] is None

        spark.conf.set("spark.sql.ansi.enabled", "true")
        with pytest.raises(Exception, match="overflow|OVERFLOW|precision"):
            df.withColumn("average", F.avg("amount").over(window)).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("frame_kind", ["rows", "range"])
def test_ansi_integer_window_sum_detects_intermediate_overflow(
    spark, frame_kind
) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    Window = get_imports().Window
    df = spark.createDataFrame(
        [(1, 9_223_372_036_854_775_807), (2, 1), (3, -1)],
        "seq int, amount bigint",
    )
    window = Window.orderBy("seq")
    if frame_kind == "rows":
        window = window.rowsBetween(0, 2)
    else:
        window = window.rangeBetween(0, 2)

    previous_ansi = spark.conf.get("spark.sql.ansi.enabled", "true")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        with pytest.raises(Exception, match="overflow|OVERFLOW|BIGINT"):
            df.withColumn("total", F.sum("amount").over(window)).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


def test_arrow_map_schema_and_nested_map_round_trip(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    from sparkless.testing import get_imports

    map_type = pa.map_(pa.string(), pa.decimal128(12, 2))
    schema = pa.schema(
        [
            (
                "details",
                pa.struct([("balances", map_type)]),
            )
        ]
    )
    table = pa.Table.from_pylist(
        [{"details": {"balances": [("checking", Decimal("10.25"))]}}],
        schema=schema,
    )

    df = spark.createDataFrame(table)
    exported = df.toArrow()

    assert isinstance(
        df.schema["details"].dataType["balances"].dataType,
        get_imports().MapType,
    )
    assert exported.schema.field("details").type == schema.field("details").type
    assert exported.to_pylist() == [
        {"details": {"balances": [("checking", Decimal("10.25"))]}}
    ]


def test_overwriting_arrow_map_with_array_does_not_reuse_stale_map_type(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    from sparkless.testing import get_imports

    F = get_imports().F
    entries_type = pa.list_(pa.struct([("key", pa.string()), ("value", pa.int64())]))
    schema = pa.schema(
        [
            ("m", pa.map_(pa.string(), pa.int64())),
            ("entries", entries_type),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "m": [("old", 1)],
                "entries": [{"key": "new", "value": 2}],
            }
        ],
        schema=schema,
    )

    replaced = spark.createDataFrame(table).withColumn("m", F.col("entries"))
    exported = replaced.toArrow()
    projected = spark.createDataFrame(table).select(F.col("entries").alias("m"))
    computed = spark.createDataFrame(table).select(
        F.coalesce(F.col("entries"), F.col("entries")).alias("m")
    )

    assert exported.schema.field("m").type == entries_type
    assert exported.to_pylist()[0]["m"] == [{"key": "new", "value": 2}]
    assert projected.toArrow().schema.field("m").type == entries_type
    assert isinstance(computed.schema["m"].dataType, get_imports().ArrayType)
    assert computed.toArrow().schema.field("m").type == entries_type


def test_decimal_pivot_sum_and_average_keep_spark_types(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    from sparkless.testing import get_imports

    F = get_imports().F
    table = pa.Table.from_pylist(
        [
            {"grp": "g", "kind": "a", "amount": Decimal("9007199254740993.01")},
            {"grp": "g", "kind": "a", "amount": Decimal("2.00")},
        ],
        schema=pa.schema(
            [
                ("grp", pa.string()),
                ("kind", pa.string()),
                ("amount", pa.decimal128(20, 2)),
            ]
        ),
    )
    df = spark.createDataFrame(table)

    convenience_sum = df.groupBy("grp").pivot("kind", ["a"]).sum("amount")
    convenience_avg = df.groupBy("grp").pivot("kind", ["a"]).avg("amount")
    aggregate = (
        df.groupBy("grp")
        .pivot("kind", ["a"])
        .agg(F.sum("amount").alias("total"), F.avg("amount").alias("average"))
    )
    single_sum = (
        df.groupBy("grp").pivot("kind", ["a"]).agg(F.sum("amount").alias("total"))
    )
    single_avg = (
        df.groupBy("grp").pivot("kind", ["a"]).agg(F.avg("amount").alias("average"))
    )

    assert convenience_sum.schema["a"].dataType.simpleString() == "decimal(30,2)"
    assert convenience_sum.collect()[0]["a"] == Decimal("9007199254740995.01")
    assert convenience_avg.schema["a"].dataType.simpleString() == "decimal(24,6)"
    assert convenience_avg.collect()[0]["a"] == Decimal("4503599627370497.505000")
    row = aggregate.collect()[0]
    assert row["a_total"] == Decimal("9007199254740995.01")
    assert row["a_average"] == Decimal("4503599627370497.505000")
    assert single_sum.schema["total"].dataType.simpleString() == "decimal(30,2)"
    assert single_sum.collect()[0]["total"] == Decimal("9007199254740995.01")
    assert single_avg.schema["average"].dataType.simpleString() == "decimal(24,6)"
    assert single_avg.collect()[0]["average"] == Decimal("4503599627370497.505000")


def test_decimal_transport_marker_does_not_change_string_values(spark) -> None:
    marker_value = "__sparkless_decimal__:1.25"

    df = spark.createDataFrame([{"amount": marker_value}], schema="amount string")

    assert df.collect()[0]["amount"] == marker_value


def test_arrow_array_of_key_value_structs_survives_transformations(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    from sparkless.testing import get_imports

    F = get_imports().F
    entry_type = pa.struct([("key", pa.string()), ("value", pa.decimal128(12, 2))])
    table = pa.Table.from_pylist(
        [
            {
                "entries": [
                    {"key": "same", "value": Decimal("1.25")},
                    {"key": "same", "value": Decimal("2.50")},
                ]
            }
        ],
        schema=pa.schema([("entries", pa.list_(entry_type))]),
    )
    df = spark.createDataFrame(table)
    expected = [
        {"key": "same", "value": Decimal("1.25")},
        {"key": "same", "value": Decimal("2.50")},
    ]

    outputs = [
        (df.withColumn("id", F.lit(1)), "entries"),
        (df.withColumn("entries", F.col("entries")), "entries"),
        (df.withColumns({"entries": F.col("entries"), "id": F.lit(1)}), "entries"),
        (df.cache(), "entries"),
        (df.persist(), "entries"),
        (df.distinct(), "entries"),
        (df.dropna(), "entries"),
        (df.repartition(1), "entries"),
        (df.union(df), "entries"),
        (df.unionByName(df), "entries"),
        (df.selectExpr("entries AS renamed"), "renamed"),
        (df.withColumnRenamed("entries", "renamed"), "renamed"),
        (df.withColumnsRenamed({"entries": "renamed"}), "renamed"),
        (df.toDF("renamed"), "renamed"),
    ]
    for output, name in outputs:
        assert output.collect()[0][name] == expected


def test_arrow_array_of_key_value_structs_inside_struct_stays_an_array(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    entry_type = pa.struct([("key", pa.string()), ("value", pa.decimal128(12, 2))])
    table = pa.Table.from_pylist(
        [
            {
                "payload": {
                    "items": [
                        {"key": "same", "value": Decimal("1.25")},
                        {"key": "same", "value": Decimal("2.50")},
                    ]
                }
            }
        ],
        schema=pa.schema([("payload", pa.struct([("items", pa.list_(entry_type))]))]),
    )

    df = spark.createDataFrame(table)
    value = df.collect()[0]["payload"]
    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)

    assert value["items"] == [
        {"key": "same", "value": Decimal("1.25")},
        {"key": "same", "value": Decimal("2.50")},
    ]
    pandas_value = df.toPandas().iloc[0]["payload"]
    assert pandas_value["items"] == value["items"]


def test_nested_arrow_maps_are_normalized_inside_structs_and_arrays(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    map_type = pa.map_(pa.string(), pa.decimal128(12, 2))
    table = pa.Table.from_pylist(
        [
            {
                "payload": {"amounts": [("a", Decimal("1.25"))]},
                "maps": [[("b", Decimal("2.50"))]],
            }
        ],
        schema=pa.schema(
            [
                ("payload", pa.struct([("amounts", map_type)])),
                ("maps", pa.list_(map_type)),
            ]
        ),
    )

    row = spark.createDataFrame(table).collect()[0]

    payload = row["payload"]
    if hasattr(payload, "asDict"):
        payload = payload.asDict(recursive=True)
    assert payload == {"amounts": {"a": Decimal("1.25")}}
    assert row["maps"] == [{"b": Decimal("2.50")}]
    pandas_row = spark.createDataFrame(table).toPandas().iloc[0]
    assert pandas_row["payload"]["amounts"] == {"a": Decimal("1.25")}
    assert pandas_row["maps"] == [{"b": Decimal("2.50")}]


def test_pandas_column_named_schema_is_not_treated_as_arrow_schema(spark) -> None:
    pd = pytest.importorskip("pandas")
    data = pd.DataFrame({"schema": ["ordinary value"], "value": [1]})

    rows = spark.createDataFrame(data).collect()

    assert rows[0]["schema"] == "ordinary value"
    assert rows[0]["value"] == 1


@pytest.mark.parametrize(
    ("schema", "value", "literal"),
    [
        (
            "amount decimal(20,18)",
            Decimal("0.123456789012345678"),
            "0.123456789012345678",
        ),
        (
            "amount decimal(20,2)",
            Decimal("9007199254740993.01"),
            "9007199254740993.01",
        ),
    ],
)
def test_sql_decimal_literals_keep_all_digits(spark, schema, value, literal) -> None:
    df = spark.createDataFrame([{"amount": value}], schema=schema)

    assert [row["amount"] for row in df.filter(f"amount = {literal}").collect()] == [
        value
    ]


def test_string_and_float_decimal_casts_use_half_up_rounding(spark) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    df = spark.createDataFrame(
        [{"text": "1.25", "number": 1.25}], schema="text string, number double"
    )

    row = df.select(
        F.col("text").cast("decimal(3,1)").alias("from_text"),
        F.col("number").cast("decimal(3,1)").alias("from_number"),
    ).collect()[0]

    assert row["from_text"] == Decimal("1.3")
    assert row["from_number"] == Decimal("1.3")


def test_grouped_decimal_sum_and_average_keep_decimal_semantics(spark) -> None:
    df = spark.createDataFrame(
        [
            {"kind": "first", "amount": Decimal("0.10")},
            {"kind": "first", "amount": Decimal("0.20")},
        ],
        schema="kind string, amount decimal(12,2)",
    )

    total = df.groupBy("kind").sum("amount")
    average = df.groupBy("kind").avg("amount")

    assert total.collect()[0]["sum(amount)"] == Decimal("0.30")
    assert average.schema["avg(amount)"].dataType.simpleString() == "decimal(16,6)"
    assert average.collect()[0]["avg(amount)"] == Decimal("0.150000")


def test_decimal_sum_overflow_obeys_ansi_mode_for_global_and_grouped_sums(
    spark,
) -> None:
    from sparkless.testing import get_imports

    F = get_imports().F
    largest = Decimal("9" * 38)
    df = spark.createDataFrame(
        [
            {"kind": "large", "amount": largest},
            {"kind": "large", "amount": largest},
        ],
        schema="kind string, amount decimal(38,0)",
    )
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled", "true")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        assert df.agg(F.sum("amount").alias("total")).collect()[0]["total"] is None
        assert df.groupBy("kind").sum("amount").collect()[0]["sum(amount)"] is None

        spark.conf.set("spark.sql.ansi.enabled", "true")
        with pytest.raises(Exception, match="decimal sum|precision|overflow"):
            df.agg(F.sum("amount").alias("total")).collect()
        with pytest.raises(Exception, match="decimal sum|precision|overflow"):
            df.groupBy("kind").sum("amount").collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize(
    ("expression", "value", "dtype"),
    [
        ("0.1 * 0.2", "0.02", "decimal(3,2)"),
        ("1.0 / 3.0", "0.333333", "decimal(8,6)"),
        ("1.0 + 2.0", "3.0", "decimal(3,1)"),
        ("2.0 - 0.5", "1.5", "decimal(3,1)"),
        ("1.0 + 2", "3.0", "decimal(3,1)"),
        ("1.0 * 2", "2.0", "decimal(4,1)"),
        ("1.0 / 2", "0.500000", "decimal(7,6)"),
        ("1.0 + CAST(2 AS BIGINT)", "3.0", "decimal(22,1)"),
        ("(0.0 - 1.0) / 6.0", "-0.166667", "decimal(9,6)"),
    ],
)
def test_sql_decimal_arithmetic_precision_and_scale(spark, expression, value, dtype):
    result = spark.sql(f"SELECT {expression} AS amount")

    assert result.collect()[0]["amount"] == Decimal(value)
    assert result.schema["amount"].dataType.simpleString() == dtype


def test_sql_decimal_arithmetic_broadcasts_literals_and_preserves_nulls(spark):
    df = spark.createDataFrame(
        [{"amount": Decimal("1.25")}, {"amount": None}, {"amount": Decimal("2.50")}],
        "amount decimal(12,2)",
    )

    result = df.selectExpr("amount * 0.2 AS product", "amount / 2.0 AS quotient")

    assert [row["product"] for row in result.collect()] == [
        Decimal("0.250"),
        None,
        Decimal("0.500"),
    ]
    assert [row["quotient"] for row in result.collect()] == [
        Decimal("0.625000"),
        None,
        Decimal("1.250000"),
    ]


def test_grouped_integer_sum_preserves_large_values_and_long_type(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    df = spark.createDataFrame(
        [{"kind": "a", "amount": 9007199254740993}, {"kind": "a", "amount": 2}],
        "kind string, amount long",
    )
    grouped = df.groupBy("kind").sum("amount")

    assert grouped.collect()[0]["sum(amount)"] == 9007199254740995
    assert grouped.schema["sum(amount)"].dataType.simpleString() == "bigint"
    assert (
        df.agg(F.sum("amount").alias("total")).collect()[0]["total"] == 9007199254740995
    )


def _arrow_duplicate_entries_frame(spark, name="entries"):
    pa = pytest.importorskip("pyarrow")
    entry = pa.struct([("key", pa.string()), ("value", pa.decimal128(12, 2))])
    values = [
        {"key": "same", "value": Decimal("1.25")},
        {"key": "same", "value": Decimal("2.50")},
    ]
    table = pa.Table.from_pylist(
        [{name: values}], schema=pa.schema([(name, pa.list_(entry))])
    )
    return spark.createDataFrame(table), values


def test_arrow_array_metadata_follows_positional_union(spark):
    left, expected = _arrow_duplicate_entries_frame(spark)
    right, _ = _arrow_duplicate_entries_frame(spark, "different_name")

    result = left.union(right)

    assert [row["entries"] for row in result.collect()] == [expected, expected]
    assert result.toArrow().to_pylist() == [{"entries": expected}] * 2


@pytest.mark.parametrize("array_on_left", [True, False])
def test_arrow_array_metadata_survives_union_missing_columns(spark, array_on_left):
    array, expected = _arrow_duplicate_entries_frame(spark)
    other = spark.createDataFrame([{"id": 1}], "id long")
    left, right = (array, other) if array_on_left else (other, array)

    result = left.unionByName(right, allowMissingColumns=True)

    expected_rows = [expected, None] if array_on_left else [None, expected]
    assert [row["entries"] for row in result.collect()] == expected_rows
    assert result.toPandas()["entries"].tolist() == expected_rows


def test_arrow_array_metadata_uses_resolved_renames_and_column_sources(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    df, expected = _arrow_duplicate_entries_frame(spark)
    outputs = [
        (df.withColumnsRenamed({"missing": "entries"}), "entries"),
        (df.withColumnsRenamed({"ENTRIES": "renamed"}), "renamed"),
        (df.withColumnRenamed("ENTRIES", "renamed"), "renamed"),
        (df.withColumn("entries", F.col("ENTRIES")), "entries"),
        (df.withColumns({"entries": F.col("ENTRIES")}), "entries"),
        (df.select(F.col("ENTRIES").alias("renamed")), "renamed"),
    ]

    for result, column in outputs:
        assert result.collect()[0][column] == expected


@pytest.mark.parametrize(
    ("expression", "name"),
    [
        ("entries renamed", "renamed"),
        ("entries AS `renamed field`", "renamed field"),
        ("ENTRIES AS renamed", "renamed"),
        ("(entries) AS renamed", "renamed"),
    ],
)
def test_select_expr_preserves_arrow_array_with_sql_aliases(spark, expression, name):
    df, expected = _arrow_duplicate_entries_frame(spark)

    assert df.selectExpr(expression).collect()[0][name] == expected


@pytest.mark.parametrize("value", [1.05, 2.55, -1.05, -2.55])
def test_float32_decimal_cast_preserves_binary_value(spark, value):
    from sparkless.testing import get_imports

    F = get_imports().F
    df = spark.createDataFrame([{"amount": value}], "amount double")
    result = df.select(F.col("amount").cast("float").cast("decimal(3,1)"))

    expected = Decimal("1.0" if abs(value) < 2 else "2.5")
    assert result.collect()[0]["amount"] == (expected if value > 0 else -expected)


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_nonfinite_float_decimal_casts_return_null_in_both_ansi_modes(spark, ansi):
    from sparkless.testing import get_imports

    F = get_imports().F
    previous = spark.conf.get("spark.sql.ansi.enabled", "true")
    try:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        df = spark.createDataFrame(
            [
                {"amount": value}
                for value in [float("nan"), float("inf"), float("-inf")]
            ],
            "amount double",
        )
        for source_type in ["float", "double"]:
            result = df.select(F.col("amount").cast(source_type).cast("decimal(3,1)"))
            assert [row["amount"] for row in result.collect()] == [None, None, None]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def test_reused_sum_expression_uses_plan_ansi_mode(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    previous = spark.conf.get("spark.sql.ansi.enabled", "true")
    df = spark.createDataFrame(
        [{"kind": "a", "amount": Decimal("9" * 38)}] * 2,
        "kind string, amount decimal(38,0)",
    )
    try:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        expression = F.sum("amount").alias("total")
        assert df.agg(expression).collect()[0]["total"] is None
        non_ansi_plan = df.select(expression)
        spark.conf.set("spark.sql.ansi.enabled", "true")
        assert non_ansi_plan.collect()[0]["total"] is None
        with pytest.raises(Exception, match="overflow|OVERFLOW|precision"):
            df.agg(expression).collect()
        with pytest.raises(Exception, match="overflow|OVERFLOW|precision"):
            df.groupBy("kind").agg(expression).collect()
        spark.conf.set("spark.sql.ansi.enabled", "false")
        assert df.agg(expression).collect()[0]["total"] is None
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def test_nested_arrow_paths_distinguish_literal_dots_from_struct_fields(spark):
    pa = pytest.importorskip("pyarrow")
    entry = pa.struct([("key", pa.string()), ("value", pa.decimal128(12, 2))])
    array_value = [{"key": "x", "value": Decimal("1.25")}]
    table = pa.Table.from_pylist(
        [{"a.b": array_value, "a": {"b": [("m", Decimal("3.50"))]}}],
        schema=pa.schema(
            [
                ("a.b", pa.list_(entry)),
                ("a", pa.struct([("b", pa.map_(pa.string(), pa.decimal128(12, 2)))])),
            ]
        ),
    )
    df = spark.createDataFrame(table)
    row = df.collect()[0]
    nested = row["a"]
    if hasattr(nested, "asDict"):
        nested = nested.asDict(recursive=True)

    assert row["a.b"] == array_value
    assert nested["b"] == {"m": Decimal("3.50")}
    assert df.toPandas().iloc[0]["a"]["b"] == {"m": Decimal("3.50")}


@pytest.mark.parametrize(
    ("allow_loss", "value", "dtype"),
    [
        ("true", "1.395556", "decimal(38,6)"),
        ("false", "1.395555555555555556", "decimal(38,18)"),
    ],
)
def test_sql_decimal_division_adjusts_large_result_precision(
    spark, allow_loss, value, dtype
):
    key = "spark.sql.decimalOperations.allowPrecisionLoss"
    previous = spark.conf.get(key, "true")
    try:
        spark.conf.set(key, allow_loss)
        df = spark.createDataFrame(
            [{"a": Decimal("3.14"), "b": Decimal("2.25")}],
            "a decimal(38,20), b decimal(38,20)",
        )
        result = df.selectExpr("a / b AS quotient")
        assert result.schema["quotient"].dataType.simpleString() == dtype
        assert result.collect()[0]["quotient"] == Decimal(value)
    finally:
        spark.conf.set(key, previous)


@pytest.mark.parametrize(
    ("allow_loss", "value", "dtype"),
    [
        ("true", "1.395556", "decimal(38,6)"),
        ("false", "1.395555555555555556", "decimal(38,18)"),
    ],
)
def test_column_decimal_division_respects_precision_loss_config(
    spark, allow_loss, value, dtype
):
    from sparkless.testing import get_imports

    F = get_imports().F
    key = "spark.sql.decimalOperations.allowPrecisionLoss"
    previous = spark.conf.get(key, "true")
    try:
        spark.conf.set(key, allow_loss)
        df = spark.createDataFrame(
            [{"a": Decimal("3.14"), "b": Decimal("2.25")}],
            "a decimal(38,20), b decimal(38,20)",
        )
        result = df.select((F.col("a") / F.col("b")).alias("quotient"))

        assert result.schema["quotient"].dataType.simpleString() == dtype
        assert result.collect()[0]["quotient"] == Decimal(value)
    finally:
        spark.conf.set(key, previous)


@pytest.mark.parametrize("use_na_fill", [False, True])
def test_scalar_numeric_fill_includes_decimal_columns(spark, use_na_fill):
    df = spark.createDataFrame(
        [{"amount": None}, {"amount": Decimal("1.25")}],
        "amount decimal(5,2)",
    )

    result = df.na.fill(0) if use_na_fill else df.fillna(0)

    assert [row["amount"] for row in result.collect()] == [
        Decimal("0.00"),
        Decimal("1.25"),
    ]


@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize("expression", ["amount * 2.0", "amount / 0.0"])
def test_sql_decimal_arithmetic_errors_obey_ansi_mode(spark, ansi, expression):
    previous = spark.conf.get("spark.sql.ansi.enabled", "true")
    try:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        df = spark.createDataFrame(
            [{"amount": Decimal("9" * 38)}], "amount decimal(38,0)"
        )
        if ansi == "true":
            with pytest.raises(
                Exception, match="overflow|OVERFLOW|zero|ZERO|precision"
            ):
                df.selectExpr(f"{expression} AS result").collect()
        else:
            assert (
                df.selectExpr(f"{expression} AS result").collect()[0]["result"] is None
            )
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def test_float_range_aggregates_reset_at_partition_boundaries(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    Window = get_imports().Window
    df = spark.createDataFrame(
        [("a", 0, 1e20), ("b", 0, 1.0)], "kind string, seq int, value double"
    )
    window = (
        Window.partitionBy("kind")
        .orderBy("seq")
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )

    rows = sorted(
        df.withColumn("total", F.sum("value").over(window))
        .withColumn("average", F.avg("value").over(window))
        .collect(),
        key=lambda row: row["kind"],
    )

    assert [row["total"] for row in rows] == [1e20, 1.0]
    assert [row["average"] for row in rows] == [1e20, 1.0]


def test_bounded_float_rows_aggregates_slide_and_recover_after_infinity(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    Window = get_imports().Window
    df = spark.createDataFrame(
        [(0, float("inf")), (1, 1.0), (2, 2.0), (3, None)],
        "seq int, value double",
    )
    window = Window.orderBy("seq").rowsBetween(-1, Window.currentRow)

    rows = [
        row
        for row in df.withColumn("total", F.sum("value").over(window))
        .withColumn("average", F.avg("value").over(window))
        .orderBy("seq")
        .collect()
    ]

    # Non-finite values are exposed as null through the JSON row transport.
    assert rows[0]["total"] is None
    assert rows[1]["total"] is None
    assert rows[2]["total"] == 3.0
    assert rows[3]["total"] == 2.0
    assert rows[2]["average"] == 1.5
    assert rows[3]["average"] == 2.0


def test_bounded_float_rows_recover_after_finite_sum_overflow(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    Window = get_imports().Window
    largest_float = float.fromhex("0x1.fffffffffffffp+1023")
    df = spark.createDataFrame(
        [(0, largest_float), (1, largest_float), (2, 1.0)],
        "seq int, value double",
    )
    window = Window.orderBy("seq").rowsBetween(-1, Window.currentRow)

    rows = [
        row
        for row in df.withColumn("total", F.sum("value").over(window))
        .orderBy("seq")
        .collect()
    ]

    assert rows[0]["total"] == largest_float
    assert rows[1]["total"] is None
    assert rows[2]["total"] == largest_float


@pytest.mark.parametrize(
    ("target", "value", "expected"),
    [
        ("smallint", Decimal("32768"), -32768),
        ("int", Decimal("2147483648"), -2147483648),
        ("bigint", Decimal("9223372036854775808"), -9223372036854775808),
    ],
)
def test_decimal_to_integer_cast_obeys_ansi_overflow_mode(
    spark, target, value, expected
):
    from sparkless.testing import get_imports

    F = get_imports().F
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled", "true")
    df = spark.createDataFrame([(value,)], "amount decimal(38,0)")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        casted = df.select(F.col("amount").cast(target).alias("value"))
        if target == "smallint":
            assert casted.schema["value"].dataType.simpleString() == "smallint"
        assert casted.collect()[0]["value"] == expected
        try_casted = df.select(F.try_cast(F.col("amount"), target).alias("value"))
        assert try_casted.collect()[0]["value"] is None

        spark.conf.set("spark.sql.ansi.enabled", "true")
        with pytest.raises(Exception, match="CAST_OVERFLOW|overflow|OVERFLOW"):
            df.select(F.col("amount").cast(target).alias("value")).collect()
        try_casted = df.select(F.try_cast(F.col("amount"), target).alias("value"))
        assert try_casted.collect()[0]["value"] is None
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


def test_decimal_average_overflow_obeys_ansi_mode_for_global_and_grouped_aggregates(
    spark,
):
    from sparkless.testing import get_imports

    F = get_imports().F
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled", "true")
    df = spark.createDataFrame(
        [("large", Decimal("1E+34"))], "kind string, amount decimal(38,0)"
    )
    try:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        expression = F.avg("amount").alias("average")
        non_ansi_plan = df.agg(expression)
        assert non_ansi_plan.collect()[0]["average"] is None
        assert df.groupBy("kind").agg(expression).collect()[0]["average"] is None

        spark.conf.set("spark.sql.ansi.enabled", "true")
        # The already analyzed non-ANSI plan retains the setting it was built with.
        assert non_ansi_plan.collect()[0]["average"] is None
        with pytest.raises(Exception, match="overflow|OVERFLOW|precision"):
            df.agg(expression).collect()
        with pytest.raises(Exception, match="overflow|OVERFLOW|precision"):
            df.groupBy("kind").agg(expression).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize(
    ("projection", "output"),
    [
        ("coalesce(entries, entries) AS m", "m"),
        ("coalesce(entries, entries)", "coalesce(entries, entries)"),
    ],
)
def test_select_expr_computed_projection_preserves_arrow_metadata(
    spark, projection, output
):
    pa = pytest.importorskip("pyarrow")
    entry_type = pa.list_(pa.struct([("key", pa.string()), ("value", pa.int32())]))
    entries = [{"key": "a", "value": 1}, {"key": "a", "value": 2}]
    table = pa.Table.from_pylist(
        [{"m": [("old", 1)], "entries": entries}],
        schema=pa.schema(
            [
                ("m", pa.map_(pa.string(), pa.int32())),
                ("entries", entry_type),
            ]
        ),
    )

    result = spark.createDataFrame(table).selectExpr(projection)

    assert result.collect()[0][output] == entries
    exported = result.toArrow()
    assert exported.schema.field(output).type == entry_type
    assert exported.to_pylist() == [{output: entries}]


def test_arrow_array_metadata_survives_struct_construction(spark):
    pa = pytest.importorskip("pyarrow")
    from sparkless.testing import get_imports

    F = get_imports().F
    entry_type = pa.list_(pa.struct([("key", pa.string()), ("value", pa.int32())]))
    entries = [{"key": "a", "value": 1}, {"key": "a", "value": 2}]
    table = pa.Table.from_pylist(
        [{"entries": entries}], schema=pa.schema([("entries", entry_type)])
    )

    result = spark.createDataFrame(table).select(F.struct("entries").alias("outer"))

    outer = result.collect()[0]["outer"]
    if hasattr(outer, "asDict"):
        outer = outer.asDict(recursive=True)
    assert outer["entries"] == entries
    exported = result.toArrow()
    assert exported.schema.field("outer").type == pa.struct([("entries", entry_type)])
    assert exported.to_pylist() == [{"outer": {"entries": entries}}]


def test_decimal_column_arithmetic_keeps_exact_values(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    df = spark.createDataFrame(
        [{"left": Decimal("9007199254740993.01"), "right": Decimal("0.01")}],
        "left decimal(20,2), right decimal(20,2)",
    )
    result = df.select(
        (F.col("left") + F.col("right")).alias("added"),
        (F.col("left") - F.col("right")).alias("subtracted"),
        (F.col("left") * F.col("right")).alias("multiplied"),
    )

    assert result.collect()[0] == {
        "added": Decimal("9007199254740993.02"),
        "subtracted": Decimal("9007199254740993.00"),
        "multiplied": Decimal("90071992547409.9301"),
    }
    assert result.schema["added"].dataType.simpleString() == "decimal(21,2)"


def test_decimal_column_division_keeps_exact_values(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    df = spark.createDataFrame(
        [{"amount": Decimal("9007199254740993.01"), "divisor": Decimal("0.01")}],
        "amount decimal(20,2), divisor decimal(20,2)",
    )

    value = df.select((F.col("amount") / F.col("divisor")).alias("quotient")).collect()[
        0
    ]["quotient"]

    assert value == Decimal("900719925474099301.000000000000000000")


def test_ansi_integer_sum_raises_on_int64_overflow(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled", "false")
    df = spark.createDataFrame(
        [{"value": 9223372036854775807}, {"value": 1}], "value long"
    )
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        with pytest.raises(Exception, match="overflow|OVERFLOW"):
            df.agg(F.sum("value").alias("total")).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


def test_decimal_column_comparison_preserves_sub_precision_values(spark):
    from sparkless.testing import get_imports

    F = get_imports().F
    df = spark.createDataFrame(
        [{"fraction": Decimal("1E-38"), "zero": 0}],
        "fraction decimal(38,38), zero int",
    )

    assert len(df.filter(F.col("fraction") > F.col("zero")).collect()) == 1
    assert df.filter(F.col("fraction") == F.col("zero")).collect() == []


def test_dtypes_reports_decimal_precision_and_scale(spark):
    df = spark.createDataFrame([{"amount": Decimal("10.50")}], "amount decimal(12,2)")

    assert df.dtypes == [("amount", "decimal(12,2)")]
