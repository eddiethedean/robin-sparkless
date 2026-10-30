"""Require an approved numeric baseline and enforce the Spark Connect V26 budget."""

from __future__ import annotations

import argparse
from datetime import date
import json
import math
from pathlib import Path
import re
import statistics
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
    replicates = approval.get("measurement_replicates")
    if (
        not isinstance(replicates, int)
        or isinstance(replicates, bool)
        or replicates < 1
    ):
        raise BudgetError(
            "V26 budget approval.measurement_replicates must be a positive integer"
        )

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


def _load_reports(report_dir: Path, role: str) -> list[dict[str, Any]]:
    paths = sorted(report_dir.rglob("benchmark.json"))
    if not paths:
        raise BudgetError(
            f"expected at least one {role} V26 benchmark report under {report_dir}, found 0"
        )
    reports: list[dict[str, Any]] = []
    for path in paths:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise BudgetError(
                f"could not read {role} V26 report {path}: {error}"
            ) from error
        if (
            not isinstance(report, dict)
            or report.get("schema_version") != 1
            or report.get("slice") != "418-08-local-v26"
        ):
            raise BudgetError(
                f"{role} report is not a supported V26 measurement: {path}"
            )
        reports.append(report)
    return reports


def _cohorts(report: dict[str, Any], role: str) -> dict[int, dict[str, Any]]:
    measurements = report.get("cohorts")
    if not isinstance(measurements, list) or len(measurements) != len(CONCURRENCIES):
        raise BudgetError(f"{role} report must contain all four V26 cohorts")
    by_concurrency = {
        item.get("concurrency"): item for item in measurements if isinstance(item, dict)
    }
    if set(by_concurrency) != set(CONCURRENCIES):
        raise BudgetError(f"{role} report has duplicate or missing V26 cohorts")
    return by_concurrency


def _settings(report: dict[str, Any], role: str) -> dict[str, Any]:
    settings = report.get("settings")
    if not isinstance(settings, dict):
        raise BudgetError(f"{role} report has no workload settings")
    if settings.get("concurrency") != list(CONCURRENCIES):
        raise BudgetError(f"{role} must measure the approved 1/2/4/8-client cohorts")
    if settings.get("regression_budget") is not None:
        raise BudgetError(
            f"{role} measurement must not embed or override the approved budget"
        )
    return settings


def verify_budget(
    report_dir: Path, budget_path: Path, baseline_report_dir: Path | None = None
) -> list[str]:
    budget = _load_budget(budget_path)
    reports = _load_reports(report_dir, "candidate")
    baseline_reports = (
        _load_reports(baseline_report_dir, "baseline")
        if baseline_report_dir is not None
        else []
    )
    approval = budget["approval"]
    expected_replicates = approval["measurement_replicates"]
    if baseline_reports:
        if (
            len(reports) != expected_replicates
            or len(baseline_reports) != expected_replicates
        ):
            raise BudgetError(
                f"expected {expected_replicates} matched baseline/candidate reports, "
                f"found {len(baseline_reports)} baseline and {len(reports)} candidate"
            )
    elif len(reports) != 1:
        raise BudgetError(
            f"expected one candidate report without a matched baseline, found {len(reports)}"
        )

    reference_environment: dict[str, Any] | None = None
    reference_settings: dict[str, Any] | None = None
    candidate_revision: str | None = None
    for role, role_reports in (("baseline", baseline_reports), ("candidate", reports)):
        for report in role_reports:
            environment = report.get("environment")
            if not isinstance(environment, dict):
                raise BudgetError(f"{role} report has no environment record")
            settings = _settings(report, role)
            _cohorts(report, role)
            if reference_settings is None:
                reference_settings = settings
            else:
                for setting in (
                    "concurrency",
                    "count_rows",
                    "collect_rows",
                    "slow_reader_rows",
                    "iterations_per_client",
                ):
                    if settings.get(setting) != reference_settings.get(setting):
                        raise BudgetError(f"{role} reports do not match on {setting}")
            if role == "baseline":
                if (
                    report.get("source_revision")
                    != approval["baseline_source_revision"]
                ):
                    raise BudgetError(
                        "matched baseline report is not from the approved source revision"
                    )
                if report.get("source_dirty_paths") != []:
                    raise BudgetError(
                        "matched baseline report must come from a clean source tree"
                    )
            else:
                if report.get("source_dirty_paths") != []:
                    raise BudgetError(
                        "candidate report must come from a clean source tree"
                    )
                revision = report.get("source_revision")
                if not isinstance(revision, str) or not revision:
                    raise BudgetError("candidate report has no source revision")
                if candidate_revision is not None and revision != candidate_revision:
                    raise BudgetError(
                        "candidate measurements use different source revisions"
                    )
                candidate_revision = revision
            if reference_environment is None:
                reference_environment = environment
            else:
                if environment.get("cpu_model") != reference_environment.get(
                    "cpu_model"
                ):
                    raise BudgetError("candidate and baseline CPU models do not match")
                if environment.get("logical_cpu_count") != reference_environment.get(
                    "logical_cpu_count"
                ):
                    raise BudgetError(
                        "candidate and baseline logical CPU counts do not match"
                    )

    if not baseline_reports:
        assert reference_environment is not None
        if (
            not isinstance(approval.get("baseline_cpu_model"), str)
            or not approval["baseline_cpu_model"].strip()
        ):
            raise BudgetError(
                "V26 budget approval.baseline_cpu_model is required without a matched report"
            )
        if reference_environment.get("cpu_model") != approval["baseline_cpu_model"]:
            raise BudgetError(
                "candidate CPU model does not match the approved V26 baseline"
            )
        if (
            reference_environment.get("logical_cpu_count")
            != approval["baseline_logical_cpu_count"]
        ):
            raise BudgetError(
                "candidate logical CPU count does not match the approved V26 baseline"
            )
    else:
        assert reference_environment is not None
        if (
            reference_environment.get("logical_cpu_count")
            != approval["baseline_logical_cpu_count"]
        ):
            raise BudgetError(
                "matched runner logical CPU count does not match the approved baseline"
            )

    assert reference_settings is not None
    if reference_settings.get("count_rows") != approval["baseline_rows"]:
        raise BudgetError("V26 row count does not match the approved workload")
    if (
        reference_settings.get("iterations_per_client")
        != approval["baseline_iterations_per_client"]
    ):
        raise BudgetError("V26 iterations do not match the approved workload")
    candidate_by_concurrency = [_cohorts(report, "candidate") for report in reports]
    baseline_by_concurrency = [
        _cohorts(report, "baseline") for report in baseline_reports
    ]

    failures: list[str] = []
    for concurrency in CONCURRENCIES:
        approved = budget["cohorts"][str(concurrency)]
        for metric, limit_name in METRICS.items():
            measured_values = [
                _number(
                    item[concurrency].get(metric),
                    f"candidate cohort {concurrency}.{metric}",
                )
                for item in candidate_by_concurrency
            ]
            baseline_values = (
                [item[concurrency].get(metric) for item in baseline_by_concurrency]
                if baseline_reports
                else [approved["baseline"][metric]]
            )
            baseline_values = [
                _number(value, f"cohort {concurrency} baseline.{metric}")
                for value in baseline_values
            ]
            measured = statistics.median(measured_values)
            baseline = statistics.median(baseline_values)
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
    source = "matched hosted baseline" if baseline_reports else "approved baseline"
    return [
        f"V26 approved regression budget passed for {len(CONCURRENCIES)} cohorts against the {source} using medians"
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
