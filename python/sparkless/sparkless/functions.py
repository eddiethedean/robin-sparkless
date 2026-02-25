# from sparkless.functions import col, F, ... -> use sql.functions
from __future__ import annotations

import sys

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
