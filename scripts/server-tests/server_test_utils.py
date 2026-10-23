from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path


def _load_server_launcher():
    # Load the lifecycle module without importing the much larger native Python
    # extension. The server launcher itself only depends on sparkless.__version__.
    package = types.ModuleType("sparkless")
    package.__path__ = [
        str(Path(__file__).resolve().parents[2] / "python" / "sparkless" / "sparkless")
    ]
    package.__version__ = "4.16.0"
    sys.modules["sparkless"] = package
    return importlib.import_module("sparkless.server").SparklessServer


SparklessServer = _load_server_launcher()
