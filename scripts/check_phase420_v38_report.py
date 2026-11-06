"""Apply the frozen Phase 4.20 resource envelope to one release report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


MIB = 1024 * 1024


def check_report(
    report: dict[str, Any], owner_mode: str, reader_mode: str
) -> list[str]:
    errors: list[str] = []
    if report.get("environment", {}).get("execution_backend") != "duckdb":
        errors.append("the measured server did not report the DuckDB execution backend")

    owner = report.get("duckdb_owner", {})
    if owner.get("mode") != owner_mode:
        errors.append(f"owner mode is {owner.get('mode')!r}, expected {owner_mode!r}")

    settings = report.get("settings", {})
    expected_settings = {
        "profile": "419",
        "concurrency": [8],
        "active_operations": 16,
        "queue_timeout_ms": 5_000,
        "count_rows": 1_000_000,
        "slow_reader_rows": 1_000_000,
        "slow_reader_pause_ms": 100.0,
        "slow_reader_reattachable": reader_mode == "reattachable",
        "duckdb_owner_mode": owner_mode,
    }
    for name, expected in expected_settings.items():
        if settings.get(name) != expected:
            errors.append(
                f"setting {name} is {settings.get(name)!r}, expected {expected!r}"
            )

    cohorts = report.get("cohorts", [])
    if len(cohorts) != 1 or cohorts[0].get("concurrency") != 8:
        errors.append(
            "the report must contain exactly one eight-client workload cohort"
        )
    elif cohorts:
        cohort = cohorts[0]
        if cohort.get("capacity_rejections"):
            errors.append("the eight-client cohort recorded capacity rejections")
        if cohort.get("correct_operations", 0) <= 0:
            errors.append("the measured cohort completed no correct operations")
        if cohort.get("rows_per_count") != 1_000_000:
            errors.append("the cohort did not use one-million-row count queries")
        if cohort.get("operations_per_second", 0) < 5:
            errors.append("the cohort fell below the 5 correct operations/second floor")
        if cohort.get("short_job_latency_p95_ms", float("inf")) > 5_000:
            errors.append("short-query p95 exceeded the 5 second budget")
        if cohort.get("health_latency_p95_ms", float("inf")) > 1_000:
            errors.append("health p95 exceeded the 1 second budget")
        if cohort.get("probe_errors"):
            errors.append("health or HTTP progress probes reported errors")
        if cohort.get("server_sampling_errors"):
            errors.append("server RSS sampling reported errors")
        server_rss_values = [
            cohort.get("server_peak_rss_bytes"),
            report.get("slow_reader", {}).get("server_peak_rss_bytes"),
        ]
        if any(
            not isinstance(value, int) or value > 512 * MIB
            for value in server_rss_values
        ):
            errors.append("server peak RSS is missing or exceeds 512 MiB")
        if owner_mode == "quack":
            owner_rss_values = [
                cohort.get("owner_peak_rss_bytes"),
                report.get("slow_reader", {}).get("owner_peak_rss_bytes"),
            ]
            if any(
                not isinstance(value, int) or value > 256 * MIB
                for value in owner_rss_values
            ):
                errors.append("Quack owner peak RSS is missing or exceeds 256 MiB")
            if cohort.get("owner_sampling_errors"):
                errors.append("Quack owner RSS sampling reported errors")

    slow_reader = report.get("slow_reader", {})
    if slow_reader.get("rows") != 1_000_000:
        errors.append("the final paced reader did not validate one million rows")
    if slow_reader.get("health_requests", 0) <= 0:
        errors.append("health probes did not run during the final paced reader")
    if slow_reader.get("health_latency_p95_ms", float("inf")) > 1_000:
        errors.append("health p95 exceeded the 1 second budget during the final reader")
    if slow_reader.get("capacity_rejection") is not None:
        errors.append("the final paced reader was capacity-rejected")
    if slow_reader.get("probe_errors"):
        errors.append("health or HTTP probes failed during the final paced reader")

    server_shutdown = report.get("server_shutdown", {})
    if (
        server_shutdown.get("graceful") is not True
        or server_shutdown.get("exit_code") != 0
    ):
        errors.append("the server did not shut down gracefully with exit code zero")
    if owner_mode == "quack":
        owner_shutdown = report.get("quack_owner_shutdown", {})
        if (
            owner_shutdown.get("graceful") is not True
            or owner_shutdown.get("exit_code") != 0
        ):
            errors.append(
                "the Quack owner did not shut down gracefully with exit code zero"
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--owner-mode", choices=("local", "quack"), required=True)
    parser.add_argument(
        "--reader-mode",
        choices=("reattachable", "non-reattachable"),
        required=True,
    )
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    errors = check_report(report, args.owner_mode, args.reader_mode)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(
        f"accepted {args.owner_mode} owner / {args.reader_mode} reader: "
        f"server RSS {report['cohorts'][0]['server_peak_rss_bytes'] / MIB:.1f} MiB, "
        f"slow reader {report['slow_reader']['rows']} rows"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
