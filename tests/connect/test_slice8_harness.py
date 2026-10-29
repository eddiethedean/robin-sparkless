from __future__ import annotations

import unittest

from run_slice8_benchmarks import cpu_time_seconds, percentile
from run_slice8_proxy_route import require_sc_url
from slice8_proxy_url import url_with_token


class Slice8HarnessTests(unittest.TestCase):
    def test_percentiles_are_nearest_rank_and_empty_is_none(self) -> None:
        self.assertIsNone(percentile([], 0.95))
        self.assertEqual(percentile([3.0, 1.0, 2.0], 0.50), 2.0)
        self.assertEqual(percentile([3.0, 1.0, 2.0], 0.95), 3.0)

    def test_ps_cpu_formats_include_days_and_fractional_seconds(self) -> None:
        self.assertEqual(cpu_time_seconds("1-02:03:04"), 93_784.0)
        self.assertEqual(cpu_time_seconds("02:03.50"), 123.5)
        self.assertEqual(cpu_time_seconds("7.25"), 7.25)

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
