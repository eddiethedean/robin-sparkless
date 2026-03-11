#!/usr/bin/env python3
"""
Run the window-like groupBy+join benchmark and print results.

Usage (from repo root, with sparkless installed):
  python scripts/run_window_benchmark.py [--rows 100000] [--groups 1000] [--repetitions 5]
  make bench-window
"""
import sys
from pathlib import Path

# Allow importing the benchmark from repo root
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from tests.benchmarks.window_like_benchmark import main, run_benchmark

if __name__ == "__main__":
    main()
