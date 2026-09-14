import pytest


@pytest.mark.sparkless_only
def test_installed_package_version_matches_runtime_version():
    from importlib.metadata import version

    from sparkless import __version__

    assert __version__ == version("sparkless")
