"""Delta DataFrameWriter.save must honor SPARKLESS_FILES_BASE."""

from __future__ import annotations

import pytest

from tests.security.conftest import SECURITY_ERROR, assert_security_error


@pytest.mark.delta
def test_delta_save_rejects_path_outside_files_base(spark, tmp_path, monkeypatch):
    files_base = tmp_path / "files_base"
    files_base.mkdir()
    outside_path = tmp_path / "outside_table"
    monkeypatch.setenv("SPARKLESS_FILES_BASE", str(files_base))

    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(SECURITY_ERROR) as exc_info:
        df.write.format("delta").mode("overwrite").save(str(outside_path))

    assert_security_error(exc_info.value, "escapes", "files base")
    assert not outside_path.exists()
