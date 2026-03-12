from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List

from parity_hunt.runner import ScenarioResult


def _norm_ws(s: str) -> str:
    return "\n".join(line.rstrip() for line in (s or "").strip().splitlines())


def _canonical_error_msg(msg: str) -> str:
    # Strip volatile details like memory addresses and file paths.
    msg = re.sub(r"0x[0-9a-fA-F]+", "0x…", msg)
    msg = re.sub(r"/[^\\s]+", "/…", msg)
    return msg.strip()


def _canonical_schema(schema: str) -> str:
    s = (schema or "").strip()
    if not s:
        return ""
    # Normalize common naming differences.
    s = s.replace('"', "")
    s = re.sub(r"\blong\b", "bigint", s)
    s = re.sub(r"\bint\b", "integer", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key_fn(r: Dict[str, Any]) -> str:
        # Stable ordering: stringify sorted key/values
        return str(sorted((k, repr(v)) for k, v in r.items()))

    return sorted(rows, key=key_fn)


def _canonicalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        # Normalize key casing? Keep as-is (PySpark is case-sensitive in output).
        # Normalize floats to stable repr.
        nr: Dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, float) and v == v:
                nr[k] = round(v, 10)
            else:
                nr[k] = v
        out.append(nr)
    return out


@dataclass
class Comparison:
    scenario_id: str
    is_mismatch: bool
    summary: str
    details: Dict[str, Any]

    def to_jsonable(self) -> dict:
        return {
            "scenario_id": self.scenario_id,
            "is_mismatch": self.is_mismatch,
            "summary": self.summary,
            "details": self.details,
        }


def diff_scenario_results(
    scenario_id: str, pys: ScenarioResult, spk: ScenarioResult
) -> Comparison:
    details: Dict[str, Any] = {}

    if pys.ok != spk.ok:
        details["pyspark_ok"] = pys.ok
        details["sparkless_ok"] = spk.ok
        details["pyspark_error"] = None if pys.error is None else pys.error.__dict__
        details["sparkless_error"] = None if spk.error is None else spk.error.__dict__
        return Comparison(
            scenario_id=scenario_id,
            is_mismatch=True,
            summary="one backend errored, the other succeeded",
            details=details,
        )

    if not pys.ok and not spk.ok:
        p_err = pys.error
        s_err = spk.error
        p_type = p_err.exc_type if p_err else ""
        s_type = s_err.exc_type if s_err else ""
        p_msg = _canonical_error_msg(p_err.message if p_err else "")
        s_msg = _canonical_error_msg(s_err.message if s_err else "")
        if (p_type, p_msg) != (s_type, s_msg):
            details["pyspark_error"] = {"type": p_type, "message": p_msg}
            details["sparkless_error"] = {"type": s_type, "message": s_msg}
            return Comparison(
                scenario_id=scenario_id,
                is_mismatch=True,
                summary="both errored but exception differs",
                details=details,
            )
        return Comparison(
            scenario_id=scenario_id,
            is_mismatch=False,
            summary="both errored (same type/message after normalization)",
            details={},
        )

    # Both ok: compare schema/data/UI
    if _canonical_schema(pys.schema or "") != _canonical_schema(spk.schema or ""):
        details["schema"] = {"pyspark": pys.schema, "sparkless": spk.schema}

    p_data = pys.data or []
    s_data = spk.data or []
    if _sort_rows(_canonicalize_rows(p_data)) != _sort_rows(_canonicalize_rows(s_data)):
        details["data"] = {"pyspark": p_data, "sparkless": s_data}

    # UI output differs a lot across engines and is usually not a parity gap.
    # Only compare UI for scenarios explicitly targeting UI behavior.
    if scenario_id.startswith("ui."):
        p_ui = pys.ui
        s_ui = spk.ui
        if p_ui and s_ui:
            ui_diffs: Dict[str, Dict[str, str]] = {}
            for k in ["show", "print_schema", "explain", "repr_df", "repr_col"]:
                pv = _norm_ws(getattr(p_ui, k, "") or "")
                sv = _norm_ws(getattr(s_ui, k, "") or "")
                if pv != sv:
                    ui_diffs[k] = {"pyspark": pv, "sparkless": sv}
            if ui_diffs:
                details["ui"] = ui_diffs

    if details:
        keys = ", ".join(sorted(details.keys()))
        return Comparison(
            scenario_id=scenario_id,
            is_mismatch=True,
            summary=f"mismatch in: {keys}",
            details=details,
        )
    return Comparison(
        scenario_id=scenario_id,
        is_mismatch=False,
        summary="match",
        details={},
    )
