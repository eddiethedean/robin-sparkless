"""Regression coverage for #1684: DataFrameWriter.save must respect Delta format."""

import tempfile
from pathlib import Path

import pytest


@pytest.mark.delta
def test_format_delta_save_creates_readable_delta_table(spark):
    df = spark.createDataFrame([(1, "alpha")], ["id", "name"])

    with tempfile.TemporaryDirectory(prefix="sparkless_issue_1684_") as temp_dir:
        path = Path(temp_dir) / "table"

        df.write.format("delta").mode("overwrite").save(str(path))

        assert (path / "_delta_log").is_dir()
        rows = spark.read.format("delta").load(str(path)).collect()
        assert [(row["id"], row["name"]) for row in rows] == [(1, "alpha")]
