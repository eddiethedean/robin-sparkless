"""Compatibility layer for `sparkless.functions`.

PySpark and upstream Sparkless support patterns like:

* `from sparkless.functions import col, lit`
* `from sparkless import functions as F`
* `from sparkless.functions.udf import UserDefinedFunction`

This module re-exports everything from `sparkless.sql.functions`, exposes
`F` and a `Functions` helper class, and installs a lightweight
`sparkless.functions.udf` submodule for tests and user code that import the
UDF class directly.
"""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import sparkless.sql.functions as _f

# Make this module behave like a package so `sparkless.functions.functions` imports work.
# (Upstream sparkless has a `sparkless/functions/functions.py` module.)
__path__ = []  # type: ignore[var-annotated]
sys.modules.setdefault(__name__ + ".functions", sys.modules[__name__])


class Functions:
    current_database = staticmethod(_f.current_database)
    current_schema = staticmethod(_f.current_schema)
    current_catalog = staticmethod(_f.current_catalog)
    current_user = staticmethod(_f.current_user)


# Re-export everything so "from sparkless.functions import col, F" works
F = _f
__all__ = list(getattr(_f, "__all__", [])) + ["F", "Functions"]
for _name in getattr(_f, "__all__", []):
    if hasattr(_f, _name):
        globals()[_name] = getattr(_f, _name)


# Submodule sparkless.functions.udf for "from sparkless.functions.udf import UserDefinedFunction"
class UserDefinedFunction:
    """PySpark-style UDF class. Use F.udf(f, returnType) or this class for withColumn(..., udf(*cols))."""

    def __init__(self, f, returnType, name=None):
        self._f = f
        self._returnType = returnType
        self._name = name
        self._wrapped = _f.udf(f, returnType)

    def __call__(self, *cols):
        return self._wrapped(*cols)


_udf_mod: Any = ModuleType("sparkless.functions.udf")
_udf_mod.__package__ = "sparkless.functions"
setattr(_udf_mod, "UserDefinedFunction", UserDefinedFunction)
sys.modules["sparkless.functions.udf"] = _udf_mod
