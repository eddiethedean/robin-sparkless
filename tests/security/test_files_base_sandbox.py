"""H-10: SPARKLESS_FILES_BASE confines general file read/write."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession


@pytest.fixture
def files_base(tmp_path, monkeypatch):
    base = tmp_path / "files_base"
    base.mkdir()
    allowed = base / "allowed.parquet"
    monkeypatch.setenv("SPARKLESS_FILES_BASE", str(base))
    spark = SparkSession.builder.appName("files-base").getOrCreate()
    df = spark.createDataFrame([{"x": 1}])
    df.write.mode("overwrite").parquet(str(allowed))
    yield spark, base, allowed
    spark.stop()
    monkeypatch.delenv("SPARKLESS_FILES_BASE", raising=False)


def test_read_rejects_path_outside_base(files_base, tmp_path, monkeypatch) -> None:
    spark, base, allowed = files_base
    outside = tmp_path / "outside.parquet"
    monkeypatch.delenv("SPARKLESS_FILES_BASE", raising=False)
    spark.createDataFrame([{"y": 2}]).write.mode("overwrite").parquet(str(outside))
    monkeypatch.setenv("SPARKLESS_FILES_BASE", str(base))
    with pytest.raises(Exception, match="escapes files base"):
        spark.read.parquet(str(outside)).collect()
    rows = spark.read.parquet(str(allowed)).collect()
    assert len(rows) == 1


def test_write_rejects_path_outside_base(files_base, tmp_path) -> None:
    spark, _base, _allowed = files_base
    outside = tmp_path / "out" / "bad.parquet"
    df = spark.createDataFrame([{"z": 3}])
    with pytest.raises(Exception, match="escapes files base"):
        df.write.mode("overwrite").parquet(str(outside))
