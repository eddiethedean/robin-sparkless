from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest

from benchmark_lock import benchmark_lock
from run_slice8_benchmarks import (
    ProgressMonitor,
    cpu_time_seconds,
    connect_capacity_rejection,
    execution_backend_environment,
    phase_trace_summary,
    redacted_execution_backend_environment,
    percentile,
    require_candidate_without_capacity_rejections,
    server_configuration,
)
from run_slice8_proxy_route import require_sc_url
from slice8_proxy_url import url_with_token


class Slice8HarnessTests(unittest.TestCase):
    def test_benchmark_lock_rejects_an_overlapping_process(self) -> None:
        with benchmark_lock():
            contender = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from benchmark_lock import benchmark_lock\n"
                    "with benchmark_lock():\n"
                    "    pass\n",
                ],
                cwd=Path(__file__).parent,
                capture_output=True,
                text=True,
                timeout=10,
            )

        self.assertNotEqual(contender.returncode, 0)
        self.assertIn("Another Sparkless benchmark holds", contender.stderr)

    def test_percentiles_are_nearest_rank_and_empty_is_none(self) -> None:
        self.assertIsNone(percentile([], 0.95))
        self.assertEqual(percentile([3.0, 1.0, 2.0], 0.50), 2.0)
        self.assertEqual(percentile([3.0, 1.0, 2.0], 0.95), 3.0)

    def test_ps_cpu_formats_include_days_and_fractional_seconds(self) -> None:
        self.assertEqual(cpu_time_seconds("1-02:03:04"), 93_784.0)
        self.assertEqual(cpu_time_seconds("02:03.50"), 123.5)
        self.assertEqual(cpu_time_seconds("7.25"), 7.25)

    def test_server_configuration_records_overridden_queue_timeout(self) -> None:
        default = server_configuration(1_000_000, 2, 5_000)
        diagnostic = server_configuration(1_000_000, 8, 30_000)

        self.assertEqual(default["SPARKLESS_QUEUE_TIMEOUT_MS"], "5000")
        self.assertEqual(diagnostic["SPARKLESS_QUEUE_TIMEOUT_MS"], "30000")
        self.assertEqual(diagnostic["SPARKLESS_MAX_ACTIVE_OPERATIONS"], "8")
        self.assertEqual(diagnostic["SPARKLESS_RESPONSE_TIMEOUT_MS"], "60000")

    def test_duckdb_benchmark_uses_a_local_persistent_owner(self) -> None:
        database = Path("target/v38-owner.duckdb")

        environment = execution_backend_environment(database)

        self.assertEqual(environment["SPARKLESS_EXECUTION_BACKEND"], "duckdb")
        self.assertIn(
            '"mode": "local"', environment["SPARKLESS_DUCKDB_EXECUTION_OWNER"]
        )
        self.assertIn(
            str(database.resolve()), environment["SPARKLESS_DUCKDB_EXECUTION_OWNER"]
        )

    def test_quack_benchmark_redacts_the_owner_token_in_report_settings(self) -> None:
        owner = {
            "mode": "quack",
            "quack_uri": "quack:localhost:9494",
            "token": "local-test-token",
            "extension_path": "/tmp/quack.duckdb_extension",
            "disable_ssl": True,
        }

        environment = execution_backend_environment(duckdb_owner_config=owner)
        redacted = redacted_execution_backend_environment(environment)

        self.assertEqual(
            json.loads(environment["SPARKLESS_DUCKDB_EXECUTION_OWNER"])["token"],
            "local-test-token",
        )
        self.assertEqual(
            json.loads(redacted["SPARKLESS_DUCKDB_EXECUTION_OWNER"])["token"],
            "[redacted]",
        )

    def test_only_compute_admission_timeouts_are_counted_as_capacity_rejections(
        self,
    ) -> None:
        self.assertEqual(
            connect_capacity_rejection(
                "StatusCode.DEADLINE_EXCEEDED: timed out waiting for Connect compute admission",
                "slow_reader",
            ),
            {
                "source": "slow_reader",
                "status": "DEADLINE_EXCEEDED",
                "diagnostic": "timed out waiting for Connect compute admission",
            },
        )
        self.assertIsNone(
            connect_capacity_rejection(
                "AssertionError: collection returned incorrect rows", "client", 0
            )
        )
        self.assertEqual(
            connect_capacity_rejection(
                "DEADLINE_EXCEEDED: Connect response stream stalled without progress for 30 seconds",
                "slow_reader",
            )["diagnostic"],
            "Connect response stream stalled without progress",
        )

    def test_paired_candidate_rejections_fail_even_when_baseline_rejections_are_allowed(
        self,
    ) -> None:
        require_candidate_without_capacity_rejections(
            {"capacity_rejection_count": 0, "cohorts": []}, pair=1
        )
        with self.assertRaisesRegex(
            RuntimeError, "paired candidate 2 had 1 capacity rejection"
        ):
            require_candidate_without_capacity_rejections(
                {
                    "capacity_rejection_count": 1,
                    "cohorts": [
                        {
                            "capacity_rejections": [
                                {
                                    "source": "slow_reader",
                                    "diagnostic": "response stream stalled",
                                }
                            ]
                        }
                    ],
                    "slow_reader": {"capacity_rejection": None},
                },
                pair=2,
            )

    def test_progress_monitor_records_http_timeout(self) -> None:
        default = ProgressMonitor("http://127.0.0.1")
        diagnostic = ProgressMonitor("http://127.0.0.1", 30)

        self.assertEqual(default.http_timeout_seconds, 5)
        self.assertEqual(diagnostic.http_timeout_seconds, 30)

    def test_phase_trace_summarizes_arrow_timing_and_buffer_high_water_marks(
        self,
    ) -> None:
        summary = phase_trace_summary(
            "sparkless_arrow_trace event=complete source_batches=2 source_rows=10 "
            "output_batches=1 serialization_ms=2.500 rechunk_ms=0.100 "
            "ipc_write_ms=1.900 schema_rewrite_ms=0.200 combine_ms=0.300 "
            "max_source_batch_bytes_estimated=128 "
            "peak_buffered_frame_bytes_estimated=512 "
            "peak_combined_frame_bytes_estimated=400 peak_ipc_buffer_bytes=256 "
            "peak_ipc_rewrite_bytes_estimated=512 serialized_ipc_bytes=256 "
            "emitted_ipc_bytes=200\n"
            "sparkless_arrow_trace event=complete source_batches=1 source_rows=4 "
            "output_batches=1 serialization_ms=1.500 rechunk_ms=0.050 "
            "ipc_write_ms=1.200 schema_rewrite_ms=0.100 combine_ms=0.200 "
            "max_source_batch_bytes_estimated=64 "
            "peak_buffered_frame_bytes_estimated=128 "
            "peak_combined_frame_bytes_estimated=100 peak_ipc_buffer_bytes=128 "
            "peak_ipc_rewrite_bytes_estimated=256 serialized_ipc_bytes=128 "
            "emitted_ipc_bytes=100"
        )

        self.assertEqual(summary["arrow_streams"], 2)
        self.assertEqual(summary["source_batches_total"], 3)
        self.assertAlmostEqual(summary["rechunk_ms_total"], 0.15)
        self.assertAlmostEqual(summary["ipc_write_ms_total"], 3.1)
        self.assertEqual(summary["peak_buffered_frame_bytes_estimated"], 512)
        self.assertEqual(summary["peak_ipc_rewrite_bytes_estimated"], 512)
        self.assertEqual(summary["serialized_ipc_bytes_total"], 384)
        self.assertEqual(summary["emitted_ipc_bytes_total"], 300)

    def test_phase_trace_reports_compute_readmission_after_a_blocked_send(self) -> None:
        summary = phase_trace_summary(
            "connect_trace event=producer_phases operation_id=ordinary-reader "
            "blocked_send_ms=0.0 admission_reacquire_ms=0.0 responses=1 rows=1\n"
            "connect_trace event=producer_phases operation_id=slow-reader "
            "admission_wait_ms=1.0 planning_ms=2.0 arrow_next_ms=3.0 "
            "replay_accounting_ms=4.0 blocked_send_ms=5.0 "
            "client_ack_wait_ms=6.0 first_batch_grace_ms=7.0 "
            "admission_reacquire_ms=8.0 responses=9 rows=10 response_bytes=11",
            slow_reader_operation_id="slow-reader",
        )

        self.assertEqual(summary["producer_phase_samples"], 2)
        self.assertEqual(summary["admission_reacquire_ms_p50"], 0.0)
        self.assertEqual(summary["admission_reacquire_ms_p95"], 8.0)
        self.assertEqual(
            summary["slow_reader_producer_phases"]["blocked_send_ms"], "5.0"
        )
        self.assertEqual(
            summary["slow_reader_producer_phases"]["admission_reacquire_ms"], "8.0"
        )

    def test_proxy_route_requires_a_clean_pinned_spark_connect_url(self) -> None:
        self.assertEqual(
            require_sc_url("sc://proxy.example:15002/"),
            ("proxy.example", 15002, "sc://proxy.example:15002"),
        )
        for url in (
            "http://proxy.example:15002",
            "sc://proxy.example",
            "sc://proxy.example:0",
            "sc://proxy.example:15002/;token=value",
            "sc://proxy.example:15002?token=value",
            "sc://user:password@proxy.example:15002",
            "sc://[::1]:15002",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                require_sc_url(url)

    def test_token_uri_parameter_is_encoded_and_replaced(self) -> None:
        self.assertEqual(
            url_with_token("sc://proxy.example:15002/;token=old;use_ssl=true", "a/b+c"),
            "sc://proxy.example:15002/;use_ssl=true;token=a%2Fb%2Bc",
        )


if __name__ == "__main__":
    unittest.main()
