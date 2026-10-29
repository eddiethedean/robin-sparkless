"""Require an approved numeric baseline and enforce the Spark Connect V26 budget."""

from __future__ import annotations

import argparse
from datetime import date
import json
import math
from pathlib import Path
import re
import sys
from typing import Any


CONCURRENCIES = (1, 2, 4, 8)
METRICS = {
    "operations_per_second": "minimum_operations_per_second_ratio",
    "latency_p95_ms": "maximum_latency_p95_ratio",
    "server_peak_rss_bytes": "maximum_server_peak_rss_ratio",
}


class BudgetError(ValueError):
    """The budget or the measured candidate does not meet the V26 contract."""


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BudgetError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise BudgetError(f"{name} must be finite and greater than zero")
    return result


def _load_budget(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise BudgetError(
            f"approved V26 budget is missing: {path}; record a reviewed baseline "
            "and numeric limits before accepting V26"
        )
    try:
        budget = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BudgetError(f"could not read V26 budget {path}: {error}") from error
    if not isinstance(budget, dict) or budget.get("schema_version") != 1:
        raise BudgetError("V26 budget must be a JSON object with schema_version 1")

    approval = budget.get("approval")
    if not isinstance(approval, dict):
        raise BudgetError("V26 budget requires an approval record")
    if (
        not isinstance(approval.get("approved_by"), str)
        or not approval["approved_by"].strip()
    ):
        raise BudgetError("V26 budget approval.approved_by must name an approver")
    approved_at = approval.get("approved_at", "")
    if not isinstance(approved_at, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}", approved_at
    ):
        raise BudgetError("V26 budget approval.approved_at must be YYYY-MM-DD")
    try:
        date.fromisoformat(approved_at)
    except ValueError as error:
        raise BudgetError(
            "V26 budget approval.approved_at must be YYYY-MM-DD"
        ) from error
    for field in ("baseline_run", "baseline_source_revision"):
        if not isinstance(approval.get(field), str) or not approval[field].strip():
            raise BudgetError(f"V26 budget approval.{field} is required")
    if (
        not isinstance(approval.get("baseline_logical_cpu_count"), int)
        or isinstance(approval["baseline_logical_cpu_count"], bool)
        or approval["baseline_logical_cpu_count"] < 1
    ):
        raise BudgetError(
            "V26 budget approval.baseline_logical_cpu_count must be a positive integer"
        )
    for field in ("baseline_rows", "baseline_iterations_per_client"):
        if (
            not isinstance(approval.get(field), int)
            or isinstance(approval[field], bool)
            or approval[field] < 1
        ):
            raise BudgetError(f"V26 budget approval.{field} must be a positive integer")

    cohorts = budget.get("cohorts")
    expected = {str(value) for value in CONCURRENCIES}
    if not isinstance(cohorts, dict) or set(cohorts) != expected:
        raise BudgetError(
            "V26 budget must define cohorts for exactly 1, 2, 4, and 8 clients"
        )
    for concurrency in CONCURRENCIES:
        item = cohorts[str(concurrency)]
        if not isinstance(item, dict):
            raise BudgetError(f"V26 budget cohort {concurrency} must be an object")
        baseline = item.get("baseline")
        limits = item.get("limits")
        if not isinstance(baseline, dict) or not isinstance(limits, dict):
            raise BudgetError(
                f"V26 budget cohort {concurrency} requires baseline and limits objects"
            )
        for metric, limit_name in METRICS.items():
            _number(baseline.get(metric), f"cohort {concurrency} baseline.{metric}")
            ratio = _number(
                limits.get(limit_name), f"cohort {concurrency} limits.{limit_name}"
            )
            if metric == "operations_per_second" and ratio > 1:
                raise BudgetError(
                    f"cohort {concurrency} throughput minimum ratio cannot exceed 1"
                )
            if metric != "operations_per_second" and ratio < 1:
                raise BudgetError(
                    f"cohort {concurrency} maximum ratio cannot be below 1"
                )
    return budget


def _load_report(report_dir: Path, role: str) -> dict[str, Any]:
    reports = sorted(report_dir.glob("run-*/benchmark.json"))
    if len(reports) != 1:
        raise BudgetError(
            f"expected exactly one {role} V26 benchmark report under {report_dir}, found {len(reports)}"
        )
    try:
        report = json.loads(reports[0].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BudgetError(
            f"could not read {role} V26 report {reports[0]}: {error}"
        ) from error
    if (
        not isinstance(report, dict)
        or report.get("schema_version") != 1
        or report.get("slice") != "418-08-local-v26"
    ):
        raise BudgetError(f"{role} report is not a supported V26 measurement")
    return report


def verify_budget(
    report_dir: Path, budget_path: Path, baseline_report_dir: Path | None = None
) -> list[str]:
    budget = _load_budget(budget_path)
    report = _load_report(report_dir, "candidate")
    baseline_report = (
        _load_report(baseline_report_dir, "baseline")
        if baseline_report_dir is not None
        else None
    )

    environment = report.get("environment")
    approval = budget["approval"]
    if not isinstance(environment, dict):
        raise BudgetError("V26 report has no environment record")
    if baseline_report is None:
        if (
            not isinstance(approval.get("baseline_cpu_model"), str)
            or not approval["baseline_cpu_model"].strip()
        ):
            raise BudgetError(
                "V26 budget approval.baseline_cpu_model is required without a matched report"
            )
        if environment.get("cpu_model") != approval["baseline_cpu_model"]:
            raise BudgetError(
                "candidate CPU model does not match the approved V26 baseline"
            )
        if (
            environment.get("logical_cpu_count")
            != approval["baseline_logical_cpu_count"]
        ):
            raise BudgetError(
                "candidate logical CPU count does not match the approved V26 baseline"
            )
    else:
        baseline_environment = baseline_report.get("environment")
        if not isinstance(baseline_environment, dict):
            raise BudgetError("baseline report has no environment record")
        if (
            baseline_report.get("source_revision")
            != approval["baseline_source_revision"]
        ):
            raise BudgetError(
                "matched baseline report is not from the approved source revision"
            )
        if baseline_report.get("source_dirty_paths") != []:
            raise BudgetError(
                "matched baseline report must come from a clean source tree"
            )
        if report.get("source_dirty_paths") != []:
            raise BudgetError("candidate report must come from a clean source tree")
        if environment.get("cpu_model") != baseline_environment.get("cpu_model"):
            raise BudgetError("candidate and baseline CPU models do not match")
        if environment.get("logical_cpu_count") != baseline_environment.get(
            "logical_cpu_count"
        ):
            raise BudgetError("candidate and baseline logical CPU counts do not match")
        if (
            environment.get("logical_cpu_count")
            != approval["baseline_logical_cpu_count"]
        ):
            raise BudgetError(
                "matched runner logical CPU count does not match the approved baseline"
            )

    settings = report.get("settings")
    if not isinstance(settings, dict) or settings.get("concurrency") != list(
        CONCURRENCIES
    ):
        raise BudgetError("candidate must measure the approved 1/2/4/8-client cohorts")
    if settings.get("regression_budget") is not None:
        raise BudgetError(
            "measurement report must not embed or override the approved budget"
        )
    if settings.get("count_rows") != approval["baseline_rows"]:
        raise BudgetError(
            "candidate row count does not match the approved V26 baseline"
        )
    if (
        settings.get("iterations_per_client")
        != approval["baseline_iterations_per_client"]
    ):
        raise BudgetError("candidate iterations do not match the approved V26 baseline")
    if baseline_report is not None:
        baseline_settings = baseline_report.get("settings")
        if not isinstance(baseline_settings, dict):
            raise BudgetError("baseline report has no workload settings")
        if baseline_settings.get("concurrency") != list(CONCURRENCIES):
            raise BudgetError(
                "baseline must measure the approved 1/2/4/8-client cohorts"
            )
        if baseline_settings.get("regression_budget") is not None:
            raise BudgetError(
                "baseline measurement must not embed or override the approved budget"
            )
        for setting in (
            "count_rows",
            "collect_rows",
            "slow_reader_rows",
            "iterations_per_client",
        ):
            if baseline_settings.get(setting) != settings.get(setting):
                raise BudgetError(f"candidate and baseline {setting} do not match")
    observed = report.get("cohorts")
    if not isinstance(observed, list) or len(observed) != len(CONCURRENCIES):
        raise BudgetError("candidate report must contain all four V26 cohorts")
    by_concurrency = {
        item.get("concurrency"): item for item in observed if isinstance(item, dict)
    }
    if set(by_concurrency) != set(CONCURRENCIES):
        raise BudgetError("candidate report has duplicate or missing V26 cohorts")
    baseline_by_concurrency: dict[int, dict[str, Any]] = {}
    if baseline_report is not None:
        baseline_cohorts = baseline_report.get("cohorts")
        if not isinstance(baseline_cohorts, list) or len(baseline_cohorts) != len(
            CONCURRENCIES
        ):
            raise BudgetError("baseline report must contain all four V26 cohorts")
        baseline_by_concurrency = {
            item.get("concurrency"): item
            for item in baseline_cohorts
            if isinstance(item, dict)
        }
        if set(baseline_by_concurrency) != set(CONCURRENCIES):
            raise BudgetError("baseline report has duplicate or missing V26 cohorts")

    failures: list[str] = []
    for concurrency in CONCURRENCIES:
        measurement = by_concurrency[concurrency]
        approved = budget["cohorts"][str(concurrency)]
        for metric, limit_name in METRICS.items():
            measured = _number(
                measurement.get(metric),
                f"candidate cohort {concurrency}.{metric}",
            )
            baseline_value = (
                approved["baseline"][metric]
                if baseline_report is None
                else baseline_by_concurrency[concurrency].get(metric)
            )
            baseline = _number(
                baseline_value, f"cohort {concurrency} baseline.{metric}"
            )
            ratio = _number(
                approved["limits"][limit_name],
                f"approved cohort {concurrency} limits.{limit_name}",
            )
            actual_ratio = measured / baseline
            passes = (
                actual_ratio >= ratio
                if metric == "operations_per_second"
                else actual_ratio <= ratio
            )
            if not passes:
                failures.append(
                    f"{concurrency}-client {metric} ratio {actual_ratio:.4f} "
                    f"violates approved limit {ratio:.4f}"
                )
    if failures:
        raise BudgetError("V26 regression budget failed: " + "; ".join(failures))
    source = (
        "matched hosted baseline"
        if baseline_report is not None
        else "approved baseline"
    )
    return [
        f"V26 approved regression budget passed for {len(CONCURRENCIES)} cohorts against the {source}"
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument(
        "--baseline-reports",
        type=Path,
        help="matched baseline report directory from the same hosted job",
    )
    parser.add_argument("--budget", type=Path, required=True)
    args = parser.parse_args()
    try:
        messages = verify_budget(args.reports, args.budget, args.baseline_reports)
    except BudgetError as error:
        print(f"V26 budget check failed: {error}", file=sys.stderr)
        return 1
    for message in messages:
        print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
