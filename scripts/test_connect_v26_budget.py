"""Tests for paired hosted-runner V26 budget enforcement."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from scripts.verify_connect_v26_budget import BudgetError, verify_budget


BASELINE_SHA = "a" * 40
METRICS = {
    "operations_per_second": 10.0,
    "latency_p95_ms": 100.0,
    "server_peak_rss_bytes": 1_000_000,
}


def _report(revision: str, cpu_model: str, scale: float = 1.0) -> dict[str, object]:
    return {
        "schema_version": 1,
        "slice": "418-08-local-v26",
        "source_revision": revision,
        "source_dirty_paths": [],
        "environment": {"cpu_model": cpu_model, "logical_cpu_count": 4},
        "settings": {
            "concurrency": [1, 2, 4, 8],
            "count_rows": 1_000_000,
            "collect_rows": 20_000,
            "slow_reader_rows": 200_000,
            "iterations_per_client": 5,
            "regression_budget": None,
        },
        "cohorts": [
            {
                "concurrency": concurrency,
                "operations_per_second": METRICS["operations_per_second"] * scale,
                "latency_p95_ms": METRICS["latency_p95_ms"] / scale,
                "server_peak_rss_bytes": METRICS["server_peak_rss_bytes"]
                * (1.0 + (1.0 - scale) * 0.75),
            }
            for concurrency in (1, 2, 4, 8)
        ],
    }


class V26PairedBudgetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.candidate_dir = self.root / "candidate"
        self.baseline_dir = self.root / "baseline"
        self.candidate_run = self.candidate_dir / "run-candidate"
        self.baseline_run = self.baseline_dir / "run-baseline"
        self.candidate_run.mkdir(parents=True)
        self.baseline_run.mkdir(parents=True)
        self.budget_path = self.root / "budget.json"
        limits = {
            "minimum_operations_per_second_ratio": 0.8,
            "maximum_latency_p95_ratio": 1.25,
            "maximum_server_peak_rss_ratio": 1.15,
        }
        budget = {
            "schema_version": 1,
            "approval": {
                "approved_by": "Release approver",
                "approved_at": "2026-10-29",
                "baseline_run": "https://github.com/example/repo/actions/runs/1",
                "baseline_source_revision": BASELINE_SHA,
                "baseline_logical_cpu_count": 4,
                "baseline_rows": 1_000_000,
                "baseline_iterations_per_client": 5,
            },
            "cohorts": {
                str(concurrency): {"baseline": METRICS, "limits": limits}
                for concurrency in (1, 2, 4, 8)
            },
        }
        self.budget_path.write_text(json.dumps(budget), encoding="utf-8")
        self._write_reports("AMD EPYC matched runner", "AMD EPYC matched runner")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_reports(self, baseline_cpu: str, candidate_cpu: str) -> None:
        (self.baseline_run / "benchmark.json").write_text(
            json.dumps(_report(BASELINE_SHA, baseline_cpu)), encoding="utf-8"
        )
        (self.candidate_run / "benchmark.json").write_text(
            json.dumps(_report("b" * 40, candidate_cpu, scale=0.8)),
            encoding="utf-8",
        )

    def test_same_job_cpu_uses_paired_baseline_and_approved_limits(self) -> None:
        self.assertEqual(
            verify_budget(self.candidate_dir, self.budget_path, self.baseline_dir),
            [
                "V26 approved regression budget passed for 4 cohorts against the matched hosted baseline"
            ],
        )

    def test_paired_reports_reject_different_cpu_models(self) -> None:
        self._write_reports("AMD EPYC 9V45", "AMD EPYC 9V74")
        with self.assertRaisesRegex(BudgetError, "CPU models do not match"):
            verify_budget(self.candidate_dir, self.budget_path, self.baseline_dir)

    def test_paired_report_must_use_approved_baseline_revision(self) -> None:
        baseline = json.loads((self.baseline_run / "benchmark.json").read_text())
        baseline["source_revision"] = "c" * 40
        (self.baseline_run / "benchmark.json").write_text(json.dumps(baseline))
        with self.assertRaisesRegex(BudgetError, "approved source revision"):
            verify_budget(self.candidate_dir, self.budget_path, self.baseline_dir)


if __name__ == "__main__":
    unittest.main()
