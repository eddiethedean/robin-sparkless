"""H-10: SPARKLESS_FILES_BASE confines general file read/write."""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

from tests.security.conftest import SECURITY_ERROR, assert_security_error

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

    with pytest.raises(SECURITY_ERROR) as exc_info:
        spark.read.parquet(str(outside)).collect()
    assert_security_error(exc_info.value, "escapes", "files base")

    rows = spark.read.parquet(str(allowed)).collect()
    assert rows == [{"x": 1}]


def test_write_rejects_path_outside_base(files_base, tmp_path) -> None:
    spark, _base, _allowed = files_base
    outside = tmp_path / "out" / "bad.parquet"
    df = spark.createDataFrame([{"z": 3}])

    with pytest.raises(SECURITY_ERROR) as exc_info:
        df.write.mode("overwrite").parquet(str(outside))
    assert_security_error(exc_info.value, "escapes", "files base")


def test_read_outside_base_does_not_leak_secret_data(
    files_base, tmp_path, monkeypatch
) -> None:
    """Outside parquet must not be readable when files base is set."""
    spark, base, _allowed = files_base
    outside = tmp_path / "secret.parquet"
    monkeypatch.delenv("SPARKLESS_FILES_BASE", raising=False)
    spark.createDataFrame([{"secret": 4242}]).write.mode("overwrite").parquet(
        str(outside)
    )
    monkeypatch.setenv("SPARKLESS_FILES_BASE", str(base))

    with pytest.raises(SECURITY_ERROR):
        rows = spark.read.parquet(str(outside)).collect()
        assert not any(r.get("secret") == 4242 for r in rows), (
            "files base sandbox leaked outside data"
        )


def test_relative_escape_via_parent_segments_rejected(
    files_base, tmp_path, monkeypatch
) -> None:
    spark, base, allowed = files_base
    outside = tmp_path / "outside.parquet"
    monkeypatch.delenv("SPARKLESS_FILES_BASE", raising=False)
    spark.createDataFrame([{"leak": 1}]).write.mode("overwrite").parquet(str(outside))
    monkeypatch.setenv("SPARKLESS_FILES_BASE", str(base))

    with pytest.raises(SECURITY_ERROR) as exc_info:
        spark.read.parquet(str(base / ".." / "outside.parquet")).collect()
    assert_security_error(exc_info.value, "escapes", "files base")

    rows = spark.read.parquet(str(allowed)).collect()
    assert rows == [{"x": 1}]
