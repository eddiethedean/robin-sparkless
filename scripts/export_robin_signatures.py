#!/usr/bin/env python3
"""
Export robin_sparkless module function and method signatures to JSON.
Requires robin_sparkless to be built and installed (maturin develop --features pyo3).
  python scripts/export_robin_signatures.py [--output docs/signatures_robin_sparkless.json]
"""

from __future__ import annotations

import inspect
import json
import sys
from typing import Any, Dict, get_type_hints


def safe_get_type_hints(obj: Any) -> dict[str, Any]:
    """Return get_type_hints(obj) or {} if introspection fails."""
    try:
        return (
            get_type_hints(obj)
            if hasattr(obj, "__annotations__") or inspect.isfunction(obj)
            else {}
        )
    except Exception:
        return {}


def param_default_repr(param: inspect.Parameter) -> Any:
    """JSON-serializable default for a parameter; None if empty or not serializable."""
    if param.default is inspect.Parameter.empty:
        return None
    if param.default is None:
        return None
    try:
        json.dumps(param.default)
        return param.default
    except (TypeError, ValueError):
        return repr(param.default)


def signature_to_dict(
    name: str, obj: Any, kind: str, class_name: str | None = None
) -> Dict[str, Any] | None:
    if not callable(obj):
        return None
    try:
        sig = inspect.signature(obj)
    except (ValueError, TypeError):
        out_err: Dict[str, Any] = {
            "name": name,
            "kind": kind,
            "args": [],
            "return_annotation": None,
            "error": "no signature",
        }
        if class_name:
            out_err["class"] = class_name
        return out_err
    hints = safe_get_type_hints(obj)
    args = []
    for pname, param in sig.parameters.items():
        if pname in ("self", "cls", "_py", "_cls"):
            continue
        ann = hints.get(pname)
        args.append(
            {
                "name": pname,
                "default": param_default_repr(param),
                "annotation": str(ann) if ann is not None else None,
                "kind": str(param.kind),
            }
        )
    return_ann = hints.get("return", sig.return_annotation)
    if return_ann is inspect.Parameter.empty:
        return_ann = None
    out: Dict[str, Any] = {
        "name": name,
        "kind": kind,
        "args": args,
        "return_annotation": str(return_ann) if return_ann is not None else None,
    }
    if class_name:
        out["class"] = class_name
    return out


def collect_module_callables(module: Any) -> list[dict]:
    out = []
    for name in sorted(dir(module)):
        if name.startswith("_"):
            continue
        obj = getattr(module, name)
        if inspect.isclass(obj):
            continue
        if callable(obj):
            d = signature_to_dict(name, obj, "function")
            if d:
                out.append(d)
    return out


def collect_class_methods(cls: Any, class_name: str) -> list[dict]:
    out = []
    for name in sorted(dir(cls)):
        if name.startswith("_"):
            continue
        try:
            obj = getattr(cls, name)
        except AttributeError:
            continue
        if callable(obj):
            d = signature_to_dict(name, obj, "method", class_name)
            if d:
                out.append(d)
    return out


def main() -> int:
    output_path = "docs/signatures_robin_sparkless.json"
    if len(sys.argv) > 1 and sys.argv[1] == "--output" and len(sys.argv) > 2:
        output_path = sys.argv[2]

    try:
        import robin_sparkless as rs
    except ImportError:
        print(
            "robin_sparkless not installed. Run: maturin develop --features pyo3",
            file=sys.stderr,
        )
        return 1

    result: Dict[str, Any] = {
        "source": "robin_sparkless",
        "functions": collect_module_callables(rs),
        "classes": {},
    }

    # Classes we care about
    for attr, class_name in [
        ("SparkSession", "SparkSession"),
        ("SparkSessionBuilder", "SparkSessionBuilder"),
        ("DataFrame", "DataFrame"),
        ("GroupedData", "GroupedData"),
        ("Column", "Column"),
        ("DataFrameStat", "DataFrameStat"),
        ("DataFrameNa", "DataFrameNa"),
        ("SortOrder", "SortOrder"),
        ("WhenBuilder", "WhenBuilder"),
        ("ThenBuilder", "ThenBuilder"),
    ]:
        if hasattr(rs, attr):
            cls = getattr(rs, attr)
            if inspect.isclass(cls):
                result["classes"][class_name] = collect_class_methods(cls, class_name)
            else:
                result["classes"][class_name] = []
        else:
            result["classes"][class_name] = []

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
