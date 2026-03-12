from __future__ import annotations

import os
import sys
import time
import datetime as dt
from contextlib import redirect_stdout
from dataclasses import dataclass
from enum import Enum
from io import StringIO
from typing import Any, Callable, Dict, List, Optional


def _ensure_java_home() -> None:
    """Best-effort JAVA_HOME setup for local PySpark runs (macOS Homebrew)."""
    if "JAVA_HOME" in os.environ and os.environ["JAVA_HOME"].strip():
        return
    candidates = [
        "/opt/homebrew/opt/openjdk@11",
        "/opt/homebrew/opt/openjdk@17",
        "/opt/homebrew/opt/openjdk",
    ]
    for candidate in candidates:
        java_bin = os.path.join(candidate, "bin", "java")
        if not os.path.exists(java_bin):
            continue
        try:
            actual_java_path = os.path.realpath(java_bin)
            actual_java_home = os.path.dirname(os.path.dirname(actual_java_path))
            if os.path.exists(os.path.join(actual_java_home, "bin", "java")):
                os.environ["JAVA_HOME"] = actual_java_home
                if actual_java_home:
                    bin_dir = os.path.join(actual_java_home, "bin")
                    if bin_dir not in os.environ.get("PATH", ""):
                        os.environ["PATH"] = f"{bin_dir}:{os.environ.get('PATH', '')}"
                return
        except Exception:
            os.environ["JAVA_HOME"] = candidate
            bin_dir = os.path.join(candidate, "bin")
            if bin_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{bin_dir}:{os.environ.get('PATH', '')}"
            return


class Backend(Enum):
    PYSPARK = "pyspark"
    SPARKLESS = "sparkless"


@dataclass(frozen=True)
class RunMeta:
    started_at_epoch_s: float
    python: str
    platform: str


@dataclass
class Scenario:
    id: str
    title: str
    fn: Callable[[Any], Any]
    tags: List[str]


@dataclass
class CapturedUI:
    show: str = ""
    print_schema: str = ""
    explain: str = ""
    repr_df: str = ""
    repr_col: str = ""


@dataclass
class CapturedError:
    exc_type: str
    message: str


@dataclass
class ScenarioResult:
    backend: str
    scenario_id: str
    ok: bool
    data: Optional[List[Dict[str, Any]]] = None
    schema: Optional[str] = None
    ui: Optional[CapturedUI] = None
    error: Optional[CapturedError] = None


@dataclass
class RunResults:
    meta: RunMeta
    by_scenario: Dict[str, Dict[str, ScenarioResult]]

    def to_jsonable(self) -> dict:
        out: dict = {"by_scenario": {}}
        for sid, per_backend in self.by_scenario.items():
            out["by_scenario"][sid] = {
                b: _sr_to_jsonable(r) for b, r in per_backend.items()
            }
        return out


def _row_to_dict(r: Any) -> Dict[str, Any]:
    # Reuse the same logic as tests/utils.py but avoid importing test helpers.
    d = r.asDict() if hasattr(r, "asDict") else dict(r)
    out: Dict[str, Any] = {}
    for k, v in d.items():
        # Make results JSON-serializable and stable across backends.
        if isinstance(v, (dt.date, dt.datetime)):
            out[k] = v.isoformat()
            continue
        if (
            v is not None
            and hasattr(v, "__iter__")
            and not isinstance(v, (str, bytes, dict))
        ):
            try:
                out[k] = list(v)
            except Exception:
                out[k] = v
        else:
            out[k] = v
    return out


def _capture_show(df: Any) -> str:
    buf = StringIO()
    with redirect_stdout(buf):
        df.show()
    return buf.getvalue()


def _capture_print_schema(df: Any) -> str:
    buf = StringIO()
    with redirect_stdout(buf):
        df.printSchema()
    return buf.getvalue()


def _capture_explain(df: Any) -> str:
    buf = StringIO()
    with redirect_stdout(buf):
        # Some engines need explicit args, PySpark supports df.explain(True)
        try:
            df.explain(True)
        except Exception:
            df.explain()
    return buf.getvalue()


def _best_effort_schema(df: Any) -> str:
    s = getattr(df, "schema", None)
    if s is None:
        return ""
    # Prefer simpleString() for stable cross-backend comparisons.
    if hasattr(s, "simpleString") and callable(s.simpleString):
        try:
            return s.simpleString()
        except Exception:
            pass
    # Fallback to json() when available (PySpark schemas).
    if hasattr(s, "json") and callable(s.json):
        try:
            return s.json()
        except Exception:
            pass
    if hasattr(s, "simpleString") and callable(s.simpleString):
        try:
            return s.simpleString()
        except Exception:
            pass
    return str(s)


def _sr_to_jsonable(r: ScenarioResult) -> dict:
    return {
        "backend": r.backend,
        "scenario_id": r.scenario_id,
        "ok": r.ok,
        "data": r.data,
        "schema": r.schema,
        "ui": None if r.ui is None else r.ui.__dict__,
        "error": None if r.error is None else r.error.__dict__,
    }


def _make_session(backend: Backend):
    if backend == Backend.PYSPARK:
        _ensure_java_home()
        # Ensure the PySpark driver and worker use the same Python interpreter.
        # Without this, local setups commonly hit PYTHON_VERSION_MISMATCH (e.g. driver
        # running in a venv but workers using system python).
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        os.environ["PYSPARK_PYTHON"] = sys.executable
        from pyspark.sql import SparkSession  # type: ignore

        # Spark may reuse an existing session/JVM from prior scenarios. If so, stop it
        # so the python executable settings above take effect for new workers.
        try:
            active = SparkSession.getActiveSession()
            if active is not None:
                active.stop()
        except Exception:
            pass

        return (
            SparkSession.builder.master("local[1]")
            .appName("parity_hunt")
            .config("spark.pyspark.driver.python", os.environ["PYSPARK_DRIVER_PYTHON"])
            .config("spark.pyspark.python", os.environ["PYSPARK_PYTHON"])
            .getOrCreate()
        )
    else:
        from sparkless.sql import SparkSession  # type: ignore

        return SparkSession.builder.appName("parity_hunt").getOrCreate()


def run_one_scenario(s: Scenario, backend: Backend) -> ScenarioResult:
    sess = _make_session(backend)
    try:
        ui = CapturedUI()
        result_obj = s.fn(sess)

        # Scenario functions should return either a DataFrame, a Column, or a tuple (df, col)
        df = None
        col = None
        if isinstance(result_obj, tuple) and len(result_obj) == 2:
            df, col = result_obj
        else:
            df = result_obj

        if df is not None:
            ui.repr_df = repr(df)
            try:
                ui.show = _capture_show(df)
            except Exception as e:
                ui.show = f"<show failed: {type(e).__name__}: {e}>"
            try:
                ui.print_schema = _capture_print_schema(df)
            except Exception as e:
                ui.print_schema = f"<printSchema failed: {type(e).__name__}: {e}>"
            try:
                ui.explain = _capture_explain(df)
            except Exception as e:
                ui.explain = f"<explain failed: {type(e).__name__}: {e}>"

        if col is not None:
            ui.repr_col = repr(col)

        data = None
        schema = None
        if df is not None and hasattr(df, "collect"):
            rows = df.collect()
            data = [_row_to_dict(r) for r in rows]
            schema = _best_effort_schema(df)

        return ScenarioResult(
            backend=backend.value,
            scenario_id=s.id,
            ok=True,
            data=data,
            schema=schema,
            ui=ui,
        )
    except Exception as e:
        return ScenarioResult(
            backend=backend.value,
            scenario_id=s.id,
            ok=False,
            error=CapturedError(exc_type=type(e).__name__, message=str(e)),
        )
    finally:
        try:
            sess.stop()
        except Exception:
            pass


def run_scenarios(scenarios: List[Scenario], backends: List[Backend]) -> RunResults:
    meta = RunMeta(
        started_at_epoch_s=time.time(),
        python=sys.version.split()[0],
        platform=sys.platform,
    )
    by_scenario: Dict[str, Dict[str, ScenarioResult]] = {}
    for s in scenarios:
        by_scenario[s.id] = {}
        for b in backends:
            by_scenario[s.id][b.value] = run_one_scenario(s, b)
    return RunResults(meta=meta, by_scenario=by_scenario)
