#!/usr/bin/env python3
"""
Export PySpark (pyspark.sql) function and method signatures to JSON.
Uses inspect.signature and optional type hints. Run with PySpark installed:
  pip install pyspark
  python scripts/export_pyspark_signatures.py [--output docs/signatures_pyspark.json]
"""
from __future__ import annotations

import inspect
import json
import sys
from typing import Any, get_type_hints

# Optional: get_type_hints can fail on some PySpark objects
def safe_get_type_hints(obj: Any) -> dict:
    try:
        return get_type_hints(obj) if hasattr(obj, "__annotations__") or inspect.isfunction(obj) else {}
    except Exception:
        return {}


def param_default_repr(param: inspect.Parameter) -> Any:
    if param.default is inspect.Parameter.empty:
        return None
    if param.default is None:
        return None
    # Repr for JSON-serializable defaults
    try:
        json.dumps(param.default)
        return param.default
    except (TypeError, ValueError):
        return repr(param.default)


def signature_to_dict(name: str, obj: Any, kind: str) -> dict | None:
    """Get signature dict for a callable. kind: 'function', 'method', etc."""
    if not callable(obj):
        return None
    try:
        sig = inspect.signature(obj)
    except (ValueError, TypeError):
        return {"name": name, "kind": kind, "args": [], "return_annotation": None, "error": "no signature"}
    hints = safe_get_type_hints(obj)
    args = []
    for pname, param in sig.parameters.items():
        if pname in ("self", "cls"):
            continue
        ann = hints.get(pname)
        args.append({
            "name": pname,
            "default": param_default_repr(param),
            "annotation": str(ann) if ann is not None else None,
            "kind": str(param.kind),
        })
    return_ann = hints.get("return", sig.return_annotation)
    if return_ann is inspect.Parameter.empty:
        return_ann = None
    return {
        "name": name,
        "kind": kind,
        "args": args,
        "return_annotation": str(return_ann) if return_ann is not None else None,
    }


def collect_module_functions(module: Any) -> list[dict]:
    out = []
    for name in sorted(dir(module)):
        if name.startswith("_"):
            continue
        obj = getattr(module, name)
        if callable(obj) and not inspect.isclass(obj):
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
            d = signature_to_dict(name, obj, "method")
            if d:
                d["class"] = class_name
                out.append(d)
    return out


def main() -> int:
    output_path = "docs/signatures_pyspark.json"
    if len(sys.argv) > 1 and sys.argv[1] == "--output" and len(sys.argv) > 2:
        output_path = sys.argv[2]

    try:
        import pyspark
        from pyspark.sql import SparkSession
        from pyspark import sql as pyspark_sql
    except ImportError as e:
        print("PySpark not installed. pip install pyspark", file=sys.stderr)
        return 1

    pyspark_version = getattr(pyspark, "__version__", "unknown")

    result = {
        "source": "pyspark",
        "pyspark_version": pyspark_version,
        "functions": [],
        "classes": {},
    }

    # 1) pyspark.sql.functions
    try:
        result["functions"] = collect_module_functions(pyspark_sql.functions)
    except Exception as e:
        result["functions_error"] = str(e)

    # 2) SparkSession
    try:
        spark = SparkSession.builder.appName("export").getOrCreate()
        result["classes"]["SparkSession"] = collect_class_methods(SparkSession, "SparkSession")
    except Exception as e:
        result["classes"]["SparkSession"] = []
        result["classes_SparkSession_error"] = str(e)
        spark = None

    # 3) SparkSession.builder (SparkSessionBuilder)
    try:
        if spark is not None:
            builder = spark.builder
            result["classes"]["SparkSessionBuilder"] = collect_class_methods(type(builder), "SparkSessionBuilder")
    except Exception as e:
        result["classes"]["SparkSessionBuilder"] = []
        result["classes_SparkSessionBuilder_error"] = str(e)

    # 4) DataFrame
    try:
        if spark is not None:
            df = spark.createDataFrame([], [])
            result["classes"]["DataFrame"] = collect_class_methods(type(df), "DataFrame")
    except Exception as e:
        result["classes"]["DataFrame"] = []
        result["classes_DataFrame_error"] = str(e)

    # 5) GroupedData
    try:
        if spark is not None:
            df = spark.createDataFrame([], [])
            gd = df.groupBy()
            result["classes"]["GroupedData"] = collect_class_methods(type(gd), "GroupedData")
    except Exception as e:
        result["classes"]["GroupedData"] = []
        result["classes_GroupedData_error"] = str(e)

    # 6) Column (from F.col("x"))
    try:
        from pyspark.sql import functions as F
        col_obj = F.col("x")
        result["classes"]["Column"] = collect_class_methods(type(col_obj), "Column")
    except Exception as e:
        result["classes"]["Column"] = []
        result["classes_Column_error"] = str(e)

    # 7) DataFrameStat, DataFrameNa
    try:
        if spark is not None:
            df = spark.createDataFrame([], [])
            result["classes"]["DataFrameStat"] = collect_class_methods(type(df.stat()), "DataFrameStat")
            result["classes"]["DataFrameNa"] = collect_class_methods(type(df.na), "DataFrameNa")
    except Exception as e:
        result["classes"].setdefault("DataFrameStat", [])
        result["classes"].setdefault("DataFrameNa", [])
        result["classes_DataFrameStat_Na_error"] = str(e)

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Wrote {output_path} (PySpark {pyspark_version})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
