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

    assert [row["amount"] for row in rows] == [Decimal("0.0000001"), Decimal("1000.0000000")]


def test_python_decimal_inference_and_decimal_string_schema(spark) -> None:
    df = spark.createDataFrame(
        [{"amount": Decimal("0.10")}, {"amount": Decimal("10.50")}]
    )

    assert df.schema["amount"].dataType.simpleString() == "decimal(4,2)"
    assert [row["amount"] for row in df.collect()] == [Decimal("0.10"), Decimal("10.50")]

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

    rows = spark.createDataFrame(table).filter(
        "amount = 9007199254740993"
    ).collect()

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

    assert large.agg(F.avg("amount").alias("average")).collect()[0]["average"] == Decimal(
        "9007199254740993.020000"
    )
    assert repeating.agg(F.avg("amount").alias("average")).collect()[0]["average"] == Decimal(
        "0.666667"
    )
    grouped_averages = {
        row["kind"]: row["average"]
        for row in grouped.groupBy("kind").agg(F.avg("amount").alias("average")).collect()
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
    df = spark.createDataFrame(
        [{"amount": largest}], schema="amount decimal(38,0)"
    )

    assert [row["amount"] for row in df.filter("amount > 0.1").collect()] == [largest]


def test_decimal_map_values_are_decimal_after_collection(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    arrow_map_type = pa.map_(pa.string(), pa.decimal128(12, 2))
    table = pa.Table.from_pylist(
        [{"balances": [("checking", Decimal("10.25"))]}],
        schema=pa.schema([("balances", arrow_map_type)]),
    )

    df = spark.createDataFrame(table)
    row = df.collect()[0]

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
        pa.Table.from_pylist(
            [{"id": 1, "amounts": [Decimal("10.25")]}], schema=schema
        )
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


def test_decimal_inference_does_not_silently_reduce_scale(spark) -> None:
    with pytest.raises(Exception, match="decimal|precision|scale"):
        spark.createDataFrame([{"amount": Decimal("1E-39")}])


def test_decimal_transport_marker_does_not_change_string_values(spark) -> None:
    marker_value = "__sparkless_decimal__:1.25"

    df = spark.createDataFrame(
        [{"amount": marker_value}], schema="amount string"
    )

    assert df.collect()[0]["amount"] == marker_value
