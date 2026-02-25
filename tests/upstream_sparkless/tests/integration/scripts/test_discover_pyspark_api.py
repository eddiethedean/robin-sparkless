from typing import Dict, List
import importlib
import json

import pytest


@pytest.mark.integration
def test_discover_pyspark_api_smoke(tmp_path, monkeypatch):
    """Smoke test ensures discovery script generates expected artifacts."""
    discover = importlib.import_module("scripts.discover_pyspark_api")
    discover = importlib.reload(discover)

    stub_versions = ["9.9.9-simulated"]
    monkeypatch.setattr(discover, "PYSPARK_VERSIONS", stub_versions)

    def fake_discover(version: str) -> Dict[str, List[str]]:
        return {
            "functions": ["fake_func", f"unique_{version.replace('-', '_')}"],
            "classes_types": ["fake_class"],
            "dataframe_methods": ["select", "fake_method"],
        }

    monkeypatch.setattr(discover, "discover_api", fake_discover)

    original_save_json = discover.save_json
    original_save_markdown = discover.save_markdown

    def redirect_save_json(matrix, output_path):
        redirected = tmp_path / output_path.name
        original_save_json(matrix, redirected)

    def redirect_save_markdown(matrix, output_path, versions):
        redirected = tmp_path / output_path.name
        original_save_markdown(matrix, redirected, versions)

    monkeypatch.setattr(discover, "save_json", redirect_save_json)
    monkeypatch.setattr(discover, "save_markdown", redirect_save_markdown)

    discover.main()

    json_path = tmp_path / "pyspark_api_matrix.json"
    markdown_path = tmp_path / "PYSPARK_FUNCTION_MATRIX.md"

    assert json_path.exists(), "JSON matrix output should be created"
    assert markdown_path.exists(), "Markdown matrix output should be created"

    matrix = json.loads(json_path.read_text())
    assert "functions" in matrix and matrix["functions"], (
        "Functions matrix should not be empty"
    )
    assert "dataframe_methods" in matrix and matrix["dataframe_methods"], (
        "DataFrame methods matrix should not be empty"
    )

    markdown_content = markdown_path.read_text()
    assert "PySpark Function & Method Availability Matrix" in markdown_content
    assert "`fake_func`" in markdown_content
    assert "9.9.9-simulated" in markdown_content
