"""Regression coverage for #1686: Delta reads must preserve decimal columns."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest


@pytest.mark.delta
def test_delta_reader_preserves_external_decimal_precision_scale_and_nulls(
    spark, tmp_path
):
    pa = pytest.importorskip("pyarrow")
    deltalake = pytest.importorskip("deltalake")

    path = tmp_path / "typed"
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("amount", pa.decimal128(12, 2)),
            ("event_date", pa.date32()),
            ("event_ts", pa.timestamp("us", tz="UTC")),
        ]
    )
    source = pa.Table.from_pylist(
        [
            {
                "id": 1,
                "amount": Decimal("10.50"),
                "event_date": date(2026, 10, 16),
                "event_ts": datetime(2026, 10, 16, 12, 30, tzinfo=timezone.utc),
            },
            {
                "id": 2,
                "amount": Decimal("2.25"),
                "event_date": date(2026, 10, 17),
                "event_ts": datetime(2026, 10, 17, 12, 30, tzinfo=timezone.utc),
            },
            {
                "id": 3,
                "amount": None,
                "event_date": date(2026, 10, 18),
                "event_ts": datetime(2026, 10, 18, 12, 30, tzinfo=timezone.utc),
            },
        ],
        schema=schema,
    )
    deltalake.write_deltalake(str(path), source)

    # Delta-rs retains the same Arrow decimal type when it reads the snapshot.
    arrow_read = deltalake.DeltaTable(str(path)).to_pyarrow_table()
    assert arrow_read.schema.field("amount").type == pa.decimal128(12, 2)

    frame = spark.read.format("delta").load(str(path))
    amount_type = frame.schema["amount"].dataType
    assert (amount_type.precision, amount_type.scale) == (12, 2)
    rows = frame.orderBy("id").collect()
    assert [row["amount"] for row in rows] == [
        Decimal("10.50"),
        Decimal("2.25"),
        None,
    ]

    frame.createOrReplaceTempView("issue_1686_decimal_source")
    projection = spark.sql(
        "SELECT id, amount FROM issue_1686_decimal_source ORDER BY id"
    )
    assert [row["amount"] for row in projection.collect()] == [
        Decimal("10.50"),
        Decimal("2.25"),
        None,
    ]

    total = spark.sql(
        "SELECT SUM(amount) AS total FROM issue_1686_decimal_source"
    ).collect()[0]["total"]
    assert total == Decimal("12.75")
